package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class AddColumnMigrator implements SchemaOperationMigrator<AddColumn> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, AddColumn operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);
		dataMappings.copy(version);

		String tableId = tableMapping.getTableId(version, tableName);
		catalog.getTable(tableId)
				.addColumn(operation.getColumnDefinition().createColumn());
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, AddColumn operation) {
		throw new UnsupportedOperationException();
	}

}
