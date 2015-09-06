package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

class AddForeignKeyMigrator implements SchemaOperationMigrator<AddForeignKey> {

	@Override
	public void migrate(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version,
			AddForeignKey operation) {
		String tableName = operation.getReferringTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);
		dataMappings.copy(version);

		String tableId = tableMapping.getTableId(version, tableName);
		Table table = catalog.getTable(tableId);

		String referencedTableName = operation.getReferencedTableName();
		String referencedTableId = tableMapping.getTableId(version, referencedTableName);
		Table referencedTable = catalog.getTable(referencedTableId);

		table.addForeignKey(operation.getReferringColumnNames())
				.named(operation.getName())
				.onDelete(operation.getOnDelete())
				.onUpdate(operation.getOnUpdate())
				.referencing(referencedTable, operation.getReferencedColumnNames());
	}

}
