package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class DropForeignKeyMigrator implements SchemaOperationMigrator<DropForeignKey> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, DropForeignKey operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, refLog, version, tableName);

		TableRef tableRef = refLog.getTableRef(version, tableName);
		Table table = catalog.getTable(tableRef.getRefId());

		ForeignKey matchedForeignKey = table.getForeignKeys().stream()
				.filter(foreignKey -> foreignKey.getForeignKeyName().equals(operation.getForeignKeyName()))
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("No foreign key exists in table: " + tableName
						+ " with name: " + operation.getForeignKeyName()));

		matchedForeignKey.drop();
	}

}
