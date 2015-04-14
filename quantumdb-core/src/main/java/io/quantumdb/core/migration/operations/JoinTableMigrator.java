package io.quantumdb.core.migration.operations;

import java.util.List;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.JoinTable;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

class JoinTableMigrator implements SchemaOperationMigrator<JoinTable> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, JoinTable operation) {
		String targetTableId = RandomHasher.generateTableId(tableMapping);
		String targetTableName = operation.getTargetTableName();

		tableMapping.copyMappingFromParent(version);
		tableMapping.add(version, targetTableName, targetTableId);
		dataMappings.copy(version);

		Table targetTable = new Table(targetTableId);
		operation.getSourceColumns().entrySet().forEach(entry -> {
			String tableAlias = entry.getKey();
			List<String> columnNames = entry.getValue();

			columnNames.forEach(columnName -> {
				String tableName = operation.getSourceTables().get(tableAlias);
				String tableId = tableMapping.getTableId(version, tableName);
				Table table = catalog.getTable(tableId);
				Column column = table.getColumn(columnName);

				targetTable.addColumn(column.copy());
			});
		});

		catalog.addTable(targetTable);
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, JoinTable operation) {
		throw new UnsupportedOperationException();
	}

}
