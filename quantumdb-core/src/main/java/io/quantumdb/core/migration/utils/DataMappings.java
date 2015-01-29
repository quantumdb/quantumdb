package io.quantumdb.core.migration.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeBasedTable;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

public class DataMappings {

	public static enum Direction {
		FORWARDS, BACKWARDS
	}

	private final Catalog catalog;
	private final TableMapping tableMapping;
	private final TreeBasedTable<Table, Table, DataMapping> mappings;

	public DataMappings(TableMapping tableMapping, Catalog catalog) {
		this.catalog = catalog;
		this.mappings = TreeBasedTable.create();
		this.tableMapping = tableMapping;
	}

	public void add(Table sourceTable, String sourceColumnName, Table targetTable,
			String targetColumnName, DataMapping.Transformation transformation) {

		checkArgument(sourceTable != null, "You must specify a 'sourceTable'.");
		checkArgument(!isNullOrEmpty(sourceColumnName), "You must specify a 'sourceColumnName'.");
		checkArgument(targetTable != null, "You must specify a 'targetTable'.");
		checkArgument(!isNullOrEmpty(targetColumnName), "You must specify a 'targetColumnName'.");
		checkArgument(transformation != null, "You must specify a 'transformation'.");

		DataMapping dataMapping = mappings.get(sourceTable, targetTable);
		if (dataMapping == null) {
			dataMapping = new DataMapping(sourceTable, targetTable);
			mappings.put(sourceTable, targetTable, dataMapping);
		}

		dataMapping.setColumnMapping(sourceColumnName, targetColumnName, transformation);
	}

	public DataMappings copy(Version version) {
		checkArgument(version != null, "You must specify a 'version'.");
		checkArgument(version.getParent() != null, "The specified 'version' must have a parent Version.");

		ImmutableSet<String> tableIds = tableMapping.getTableIds(version.getParent());

		Map<Table, Table> tableMap = tableIds.stream()
				.collect(Collectors.toMap(catalog::getTable, tableId -> {
					String tableName = tableMapping.getTableName(version.getParent(), tableId);
					String newerTableId = tableMapping.getTableId(version, tableName);
					return catalog.getTable(newerTableId);
				}));

		for (Table source : tableMap.keySet()) {
			Table target = tableMap.get(source);

			for (Column sourceColumn : source.getColumns()) {
				Column targetColumn = target.getColumn(sourceColumn.getName());

				Table sourceTable = sourceColumn.getParent();
				Table targetTable = targetColumn.getParent();
				String sourceColumnName = sourceColumn.getName();
				String targetColumnName = targetColumn.getName();

				add(sourceTable, sourceColumnName, targetTable, targetColumnName, DataMapping.Transformation.createNop());
			}
		}

		return this;
	}

	public DataMappings drop(Table table) {
		checkArgument(table != null, "You must specify a 'table'.");

		Set<Table> referencedTables = mappings.row(table).keySet();
		for (Table referencedTable : referencedTables) {
			mappings.remove(table, referencedTable);
		}

		return this;
	}

	public DataMappings drop(Table table, String columnName) {
		checkArgument(table != null, "You must specify a 'table'.");
		checkArgument(!isNullOrEmpty(columnName), "You must specify a 'columnName'.");

		for (Map.Entry<Table, DataMapping> entry : mappings.row(table).entrySet()) {
			DataMapping dataMapping = entry.getValue();
			dataMapping.drop(columnName);
		}

		return this;
	}

	// TODO: Test extensively!!!
	public Set<DataMapping> getTransitiveDataMappings(Table table, Direction direction) {
		checkArgument(table != null, "You must specify a 'table'.");
		checkArgument(direction != null, "You must specify a 'direction'.");

		Map<Table, DataMapping> currentMappings = Maps.newHashMap();
		Set<Table> intermediateTables = Sets.newHashSet();

		List<DataMapping> toProcess = Lists.newArrayList();
		toProcess.addAll(getMappings(table, direction));
		while (!toProcess.isEmpty()) {
			DataMapping dataMapping = toProcess.remove(0);
			Table sourceTable = dataMapping.getSourceTable(direction);
			boolean isFirstDegreeMapping = sourceTable.equals(table);

			DataMapping currentMapping = dataMapping;
			if (!isFirstDegreeMapping) {
				intermediateTables.add(sourceTable);
				currentMapping = currentMappings.get(sourceTable);
			}

			DataMapping newMapping = currentMappings.get(dataMapping.getTargetTable(direction));
			if (newMapping == null) {
				newMapping = new DataMapping(table, dataMapping.getTargetTable(direction));
				currentMappings.put(dataMapping.getTargetTable(direction), newMapping);
			}

			Map<String, DataMapping.ColumnMapping> newColumnMappings = newMapping.getColumnMappings();
			Map<String, DataMapping.ColumnMapping> columnMappings = dataMapping.getColumnMappings();

			Map<String, DataMapping.ColumnMapping> currentColumnMappings = currentMapping.getColumnMappings();
			for (Map.Entry<String, DataMapping.ColumnMapping> entry : currentColumnMappings.entrySet()) {
				String currentColumnName = entry.getKey();
				if (!isFirstDegreeMapping) {
					currentColumnName = currentColumnMappings.get(currentColumnName).getColumnName();
				}

				DataMapping.ColumnMapping currentColumnMapping = columnMappings.get(currentColumnName);
				String newColumnName = currentColumnMapping.getColumnName();
				DataMapping.Transformation transformation = currentColumnMapping.getTransformation();

				DataMapping.Transformation newTransformation = entry.getValue().getTransformation().apply(transformation);
				DataMapping.ColumnMapping columnMapping = new DataMapping.ColumnMapping(newColumnName, newTransformation);
				newColumnMappings.put(entry.getKey(), columnMapping);
			}

			Table targetTable = dataMapping.getTargetTable(direction);
			toProcess.addAll(getMappings(targetTable, direction));
		}

		intermediateTables.forEach(currentMappings::remove);
		return Sets.newHashSet(currentMappings.values());
	}

	private Collection<DataMapping> getMappings(Table table, Direction direction) {
		checkArgument(table != null, "You must specify a 'table'.");
		checkArgument(direction != null, "You must specify a 'direction'.");

		switch (direction) {
			case FORWARDS:
				return mappings.row(table).values();
			case BACKWARDS:
				return mappings.column(table).values();
			default:
				String supportedDirections = Joiner.on(", ").join(new Direction[] {
						Direction.FORWARDS, Direction.BACKWARDS });

				throw new IllegalArgumentException("Only directions: " + supportedDirections + " are supported!");
		}
	}

}
