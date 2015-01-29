package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.Version;

class CreateTableMigrator implements SchemaOperationMigrator<CreateTable> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, CreateTable operation) {
		String tableId = RandomHasher.generateTableId(tableMapping);
		String tableName = operation.getTableName();

		tableMapping.copyMappingFromParent(version);
		tableMapping.set(version, tableName, tableId);
		dataMappings.copy(version);

		Table table = new Table(tableId);
		operation.getColumns().stream()
				.forEachOrdered(c -> table.addColumn(c.createColumn()));

		catalog.addTable(table);
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, CreateTable operation) {
		throw new UnsupportedOperationException();
	}

}
