package io.quantumdb.core.migration.operations;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.state.RefLog;
import io.quantumdb.core.state.RefLog.TableRef;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class TransitiveTableMirrorer {

	static Set<String> mirror(Catalog catalog, RefLog refLog, Version version, String... tableNames) {
		refLog.prepareFork(version);
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

			String newTableId = RandomHasher.generateTableId(refLog);
			Table table = catalog.getTable(tableRef.getTableId());
			tableRef.ghost(newTableId, version);
			catalog.addTable(table.copy().rename(newTableId));

			mirrored.add(tableRef.getName());

			// Traverse incoming foreign keys
			Set<String> referencingTableIds = catalog.getTablesReferencingTable(tableRef.getTableId());
			tablesToMirror.addAll(referencingTableIds.stream()
					.map(tableId -> refLog.getTableRefById(parentVersion, tableId))
					.collect(Collectors.toList()));
		}

		// Copying foreign keys for each affected table.
		for(String tableName : mirrored) {
			String oldTableId = refLog.getTableRef(parentVersion, tableName).getTableId();
			String newTableId = refLog.getTableRef(version, tableName).getTableId();

			Table oldTable = catalog.getTable(oldTableId);
			Table newTable = catalog.getTable(newTableId);

			List<ForeignKey> outgoingForeignKeys = oldTable.getForeignKeys();
			for (ForeignKey foreignKey : outgoingForeignKeys) {
				String oldReferredTableId = foreignKey.getReferredTableName();
				String oldReferredTableName = refLog.getTableRefById(parentVersion, oldReferredTableId).getName();
				String newReferredTableId = refLog.getTableRef(version, oldReferredTableName).getTableId();

				Table newReferredTable = catalog.getTable(newReferredTableId);
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
