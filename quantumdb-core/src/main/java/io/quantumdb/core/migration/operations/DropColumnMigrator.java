package io.quantumdb.core.migration.operations;

import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Index;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class DropColumnMigrator implements SchemaOperationMigrator<DropColumn> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, DropColumn operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, refLog, version, tableName);

		TableRef tableRef = refLog.getTableRef(version, tableName);
		String refId = tableRef.getRefId();
		tableRef.dropColumn(operation.getColumnName());

		Table table = catalog.getTable(refId);
		table.removeColumn(operation.getColumnName());

		List<Index> indexes = Lists.newArrayList(table.getIndexes());
		for (Index index : indexes) {
			table.removeIndex(index.getColumns());
		}
	}

}
