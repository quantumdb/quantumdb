package io.quantumdb.core.migration.operations;

import java.util.Map.Entry;
import java.util.stream.Collectors;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.CopyTable;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class CopyTableMigrator implements SchemaOperationMigrator<CopyTable> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, CopyTable operation) {
		String refId = RandomHasher.generateRefId(refLog);
		String sourceTableName = operation.getSourceTableName();
		String targetTableName = operation.getTargetTableName();

		refLog.fork(version);
		TableRef sourceTableRef = refLog.getTableRef(version.getParent(), sourceTableName);
		refLog.addTable(targetTableName, refId, version, sourceTableRef.getColumns().entrySet().stream()
				.map(Entry::getValue)
				.map(ColumnRef::ghost)
				.collect(Collectors.toList()));

		Table sourceTable = catalog.getTable(sourceTableRef.getRefId());
		Table targetTable = sourceTable.copy().rename(refId);
		for (ForeignKey foreignKey : sourceTable.getForeignKeys()) {
			String referredRefId = foreignKey.getReferredTableName();

			Table referredTable = catalog.getTable(referredRefId);
			targetTable.addForeignKey(foreignKey.getReferencingColumns())
					.named(foreignKey.getForeignKeyName())
					.onUpdate(foreignKey.getOnUpdate())
					.onDelete(foreignKey.getOnDelete())
					.referencing(referredTable, foreignKey.getReferredColumns());
		}

		catalog.addTable(targetTable);
	}

}
