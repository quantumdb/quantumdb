package io.quantumdb.core.migration.operations;

import java.util.Optional;

import com.google.common.base.Strings;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.versioning.Version;

class AlterColumnMigrator implements SchemaOperationMigrator<AlterColumn> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, AlterColumn operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);
		dataMappings.copy(version);

		String tableId = tableMapping.getTableId(version, tableName);
		Column column = catalog.getTable(tableId)
				.getColumn(operation.getColumnName());

		operation.getNewColumnName().ifPresent(columnName -> {
			column.rename(columnName);
		});

		operation.getHintsToDrop().stream()
				.forEach(hint -> column.dropHint(hint));

		operation.getHintsToAdd().stream()
				.forEach(hint -> column.addHint(hint));

		operation.getNewColumnType().ifPresent(columnType -> {
			column.modifyType(columnType);
		});

		Optional<String> newDefaultValueExpression = operation.getNewDefaultValueExpression();
		if (newDefaultValueExpression.isPresent()) {
			String defaultValue = newDefaultValueExpression.get();
			if (Strings.isNullOrEmpty(defaultValue)) {
				column.dropDefaultValue();
			}
			else {
				column.modifyDefaultValue(defaultValue);
			}
		}

		// TODO: replace pipeline with correct transformation
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, AlterColumn operation) {
		throw new UnsupportedOperationException();
	}

}
