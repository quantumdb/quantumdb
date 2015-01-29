package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.versioning.Version;

class DropColumnMigrator implements SchemaOperationMigrator<DropColumn> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, DropColumn operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);

		String tableId = tableMapping.getTableId(version, tableName);
		Table table = catalog.getTable(tableId);
		dataMappings.copy(version)
				.drop(table, operation.getColumnName());

		table.removeColumn(operation.getColumnName());
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, DropColumn operation) {
		throw new UnsupportedOperationException();
	}

}
