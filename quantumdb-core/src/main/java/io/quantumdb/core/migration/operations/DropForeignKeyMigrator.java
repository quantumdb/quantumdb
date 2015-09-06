package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

class DropForeignKeyMigrator implements SchemaOperationMigrator<DropForeignKey> {

	@Override
	public void migrate(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version,
			DropForeignKey operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);
		dataMappings.copy(version);

		String tableId = tableMapping.getTableId(version, tableName);
		Table table = catalog.getTable(tableId);

		ForeignKey matchedForeignKey = table.getForeignKeys().stream()
				.filter(foreignKey -> foreignKey.getForeignKeyName().equals(operation.getForeignKeyName()))
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("No foreign key exists in table: " + tableName
						+ " with name: " + operation.getForeignKeyName()));

		matchedForeignKey.drop();
	}

}
