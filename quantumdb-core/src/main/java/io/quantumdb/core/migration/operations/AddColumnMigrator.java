package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.state.RefLog;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class AddColumnMigrator implements SchemaOperationMigrator<AddColumn> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, AddColumn operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);
		refLog.prepareFork(version);

		String tableId = tableMapping.getTableId(version, tableName);
		catalog.getTable(tableId)
				.addColumn(operation.getColumnDefinition().createColumn());
	}

}
