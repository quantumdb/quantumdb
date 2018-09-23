package io.quantumdb.core.migration.operations;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class TransitiveTableMirrorer {

	static Set<String> mirror(Catalog catalog, RefLog refLog, Version version, String... tableNames) {
		refLog.fork(version);
		Version parentVersion = version.getParent();
		Set<String> mirrored = Sets.newHashSet();

		List<TableRef> tablesToMirror = Arrays.stream(tableNames)
				.map(tableName -> refLog.getTableRef(parentVersion, tableName))
				.collect(Collectors.toList());

		while(!tablesToMirror.isEmpty()) {
			TableRef tableRef = tablesToMirror.remove(0);
			if (mirrored.contains(tableRef.getName())) {
				continue;
			}

			String newRefId = RandomHasher.generateRefId(refLog);
			Table table = catalog.getTable(tableRef.getRefId());
			tableRef.ghost(newRefId, version);
			catalog.addTable(table.copy().rename(newRefId));

			mirrored.add(tableRef.getName());

			// Traverse incoming foreign keys
			Set<String> referencingRefIds = catalog.getTablesReferencingTable(tableRef.getRefId());
			tablesToMirror.addAll(referencingRefIds.stream()
					.map(refId -> refLog.getTableRefById(refId))
					.collect(Collectors.toList()));
		}

		// Copying foreign keys for each affected table.
		for(String tableName : mirrored) {
			String oldRefId = refLog.getTableRef(parentVersion, tableName).getRefId();
			String newRefId = refLog.getTableRef(version, tableName).getRefId();

			Table oldTable = catalog.getTable(oldRefId);
			Table newTable = catalog.getTable(newRefId);

			List<ForeignKey> outgoingForeignKeys = Lists.newArrayList(oldTable.getForeignKeys());
			for (ForeignKey foreignKey : outgoingForeignKeys) {
				String oldReferredRefId = foreignKey.getReferredTableName();
				String oldReferredTableName = refLog.getTableRefById(oldReferredRefId).getName();
				String newReferredRefId = refLog.getTableRef(version, oldReferredTableName).getRefId();

				Table newReferredTable = catalog.getTable(newReferredRefId);
				newTable.addForeignKey(foreignKey.getReferencingColumns())
						.named(foreignKey.getForeignKeyName())
						.onUpdate(foreignKey.getOnUpdate())
						.onDelete(foreignKey.getOnDelete())
						.referencing(newReferredTable, foreignKey.getReferredColumns());
			}
		}

		return mirrored;
	}

}
