package io.quantumdb.core.migration.operations;

import java.util.stream.Collectors;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class CreateTableMigrator implements SchemaOperationMigrator<CreateTable> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, CreateTable operation) {
		String refId = RandomHasher.generateRefId(refLog);
		String tableName = operation.getTableName();

		refLog.fork(version);
		refLog.addTable(tableName, refId, version, operation.getColumns().stream()
				.map(column -> new ColumnRef(column.getName()))
				.collect(Collectors.toList()));

		Table table = new Table(refId);
		operation.getColumns().forEach(c -> table.addColumn(c.createColumn()));

		catalog.addTable(table);
	}

}
