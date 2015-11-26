package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.state.RefLog;
import io.quantumdb.core.state.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class AddColumnMigrator implements SchemaOperationMigrator<AddColumn> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, AddColumn operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, refLog, version, tableName);

		TableRef tableRef = refLog.getTableRef(version, tableName);
		catalog.getTable(tableRef.getTableId())
				.addColumn(operation.getColumnDefinition().createColumn());
	}

}
