package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.versioning.Version;

class AddForeignKeyMigrator implements SchemaOperationMigrator<AddForeignKey> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, AddForeignKey operation) {
		String tableName = operation.getReferringTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);
		dataMappings.copy(version);

		String tableId = tableMapping.getTableId(version, tableName);
		Table table = catalog.getTable(tableId);

		String referencedTableName = operation.getReferencedTableName();
		String referencedTableId = tableMapping.getTableId(version, referencedTableName);
		Table referencedTable = catalog.getTable(referencedTableId);

		table.addForeignKey(operation.getReferringColumnNames())
				.referencing(referencedTable, operation.getReferencedColumnNames());
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, AddForeignKey operation) {
		throw new UnsupportedOperationException();
	}

}
