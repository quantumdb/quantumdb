package io.quantumdb.core.migration.operations;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Index;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class AlterColumnMigrator implements SchemaOperationMigrator<AlterColumn> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, AlterColumn operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, refLog, version, tableName);

		TableRef tableRef = refLog.getTableRef(version, tableName);
		Table table = catalog.getTable(tableRef.getRefId());
		Column column = table.getColumn(operation.getColumnName());

		operation.getNewColumnName().ifPresent(columnName -> {
			tableRef.renameColumn(operation.getColumnName(), columnName);
			column.rename(columnName);

			List<Index> indexes = Lists.newArrayList(table.getIndexes());
			for (Index index : indexes) {
				table.removeIndex(index.getColumns());
				List<String> columns = index.getColumns().stream()
						.map(this::normalize)
						.map(refColumn -> {
							if (refColumn.equals(operation.getColumnName())) {
								return columnName;
							}
							return refColumn;
						})
						.collect(Collectors.toList());

				table.addIndex(new Index(columns, index.isUnique()));
			}
		});

		operation.getHintsToDrop().stream().forEach(column::dropHint);
		operation.getHintsToAdd().stream().forEach(column::addHint);
		operation.getNewColumnType().ifPresent(column::modifyType);

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
	}

	private String normalize(String input) {
		String trimmed = input.trim();

		if (!trimmed.isEmpty()) {
			char first = trimmed.charAt(0);
			char last = trimmed.charAt(trimmed.length() - 1);

			if (first == last) {
				if (first == '\'') {
					return trimmed.substring(1, trimmed.length() - 1);
				}
				else if (last == '\"') {
					return trimmed.substring(1, trimmed.length() - 1);
				}
			}
		}

		return trimmed;
	}

}
