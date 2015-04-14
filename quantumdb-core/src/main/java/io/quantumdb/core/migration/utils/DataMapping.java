package io.quantumdb.core.migration.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import io.quantumdb.core.schema.definitions.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class DataMapping {

	@Data
	@AllArgsConstructor
	public static class ColumnMapping {
		private final String columnName;
		private final Transformation transformation;
	}

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
	private final LinkedHashMap<String, ColumnMapping> columnMappings = Maps.newLinkedHashMap();

	public Table getTargetTable(DataMappings.Direction direction) {
		switch (direction) {
			case FORWARDS:
				return targetTable;
			case BACKWARDS:
				return sourceTable;
			default:
				String supportedDirections = Joiner.on(", ").join(new DataMappings.Direction[] {
						DataMappings.Direction.FORWARDS, DataMappings.Direction.BACKWARDS });

				throw new IllegalArgumentException("Only directions: " + supportedDirections + " are supported!");
		}
	}

	public Table getSourceTable(@Nullable DataMappings.Direction direction) {
		switch (direction) {
			case FORWARDS:
				return sourceTable;
			case BACKWARDS:
				return targetTable;
			default:
				String supportedDirections = Joiner.on(", ").join(new DataMappings.Direction[] {
						DataMappings.Direction.FORWARDS, DataMappings.Direction.BACKWARDS });

				throw new IllegalArgumentException("Only directions: " + supportedDirections + " are supported!");
		}
	}

	public DataMapping setColumnMapping(String sourceColumnName, String targetColumnName, Transformation transformation) {
		checkArgument(!isNullOrEmpty(sourceColumnName), "You must specify a 'sourceColumnName'.");
		checkArgument(!isNullOrEmpty(targetColumnName), "You must specify a 'targetColumnName'.");
		checkArgument(transformation != null, "You must specify a 'transformation'.");

		columnMappings.put(sourceColumnName, new ColumnMapping(targetColumnName, transformation));
		return this;
	}

	public boolean drop(String columnName) {
		checkArgument(!isNullOrEmpty(columnName), "You must specify a 'columnName'.");
		return columnMappings.remove(columnName) != null;
	}

}
