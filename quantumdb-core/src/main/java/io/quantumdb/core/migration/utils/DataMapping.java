package io.quantumdb.core.migration.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table.Cell;
import io.quantumdb.core.schema.definitions.Table;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class DataMapping {

	@Data
	@NoArgsConstructor(access = AccessLevel.PRIVATE)
	public static class Transformation {

		public static Transformation createNop() {
			return new Transformation();
		}

		public Transformation apply(Transformation transformation) {
			// TODO: Implement...
			return createNop();
		}
	}

	private final Table sourceTable;
	private final Table targetTable;
	private final com.google.common.collect.Table<String, String, Transformation> columnMappings = HashBasedTable.create();

	public DataMapping setColumnMapping(String sourceColumnName, String targetColumnName, Transformation transformation) {
		checkArgument(!isNullOrEmpty(sourceColumnName), "You must specify a 'sourceColumnName'.");
		checkArgument(!isNullOrEmpty(targetColumnName), "You must specify a 'targetColumnName'.");
		checkArgument(transformation != null, "You must specify a 'transformation'.");

		columnMappings.put(sourceColumnName, targetColumnName, transformation);
		return this;
	}

	public boolean drop(String columnName) {
		checkArgument(!isNullOrEmpty(columnName), "You must specify a 'columnName'.");

		return columnMappings.row(columnName).keySet().stream()
				.map(other -> columnMappings.remove(columnName, other) != null)
				.reduce((left, right) -> left || right)
				.orElse(false);
	}

	public static DataMapping copyAndInverse(DataMapping input) {
		DataMapping dataMapping = new DataMapping(input.getTargetTable(), input.getSourceTable());
		for (Cell<String, String, Transformation> entry : input.getColumnMappings().cellSet()) {
			dataMapping.columnMappings.put(entry.getColumnKey(), entry.getRowKey(), entry.getValue());
		}
		return dataMapping;
	}

}
