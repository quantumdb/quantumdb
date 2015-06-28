package io.quantumdb.core.migration.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.TreeBasedTable;
import io.quantumdb.core.migration.utils.DataMapping.Transformation;
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

		Set<Table> dereferenceTables = Sets.newHashSet();
		Set<Entry<Table, DataMapping>> entries = ImmutableSet.<Entry<Table, DataMapping>>builder()
				.addAll(mappings.column(table).entrySet())
				.addAll(mappings.row(table).entrySet())
				.build();

		for (Map.Entry<Table, DataMapping> entry : entries) {
			DataMapping dataMapping = entry.getValue();
			dataMapping.drop(columnName);

			if (dataMapping.getColumnMappings().isEmpty()) {
				dereferenceTables.add(dataMapping.getTargetTable());
			}
		}

		for (Table targetTable : dereferenceTables) {
			mappings.remove(table, targetTable);
			mappings.remove(targetTable, table);
		}

		return this;
	}

	// TODO: Test extensively!!!
	public Set<DataMapping> getTransitiveDataMappings(Table table, Direction direction) {
		checkArgument(table != null, "You must specify a 'table'.");
		checkArgument(direction != null, "You must specify a 'direction'.");

		Map<Table, DataMapping> currentMappings = Maps.newHashMap();
		Set<Table> intermediateTables = Sets.newHashSet();
		Set<DataMapping> processed = Sets.newHashSet();

		List<DataMapping> toProcess = Lists.newArrayList();
		toProcess.addAll(getMappings(table, direction));
		while (!toProcess.isEmpty()) {
			DataMapping dataMapping = toProcess.remove(0);
			processed.add(dataMapping);

			Table sourceTable = dataMapping.getSourceTable();
			Table targetTable = dataMapping.getTargetTable();
			boolean isFirstDegreeMapping = sourceTable.equals(table);

			DataMapping currentMapping = dataMapping;
			if (!isFirstDegreeMapping) {
				if (!sourceTable.equals(targetTable)) {
					intermediateTables.add(sourceTable);
				}
				currentMapping = currentMappings.get(sourceTable);
			}

			DataMapping newMapping = currentMappings.get(dataMapping.getTargetTable());
			if (newMapping == null) {
				newMapping = new DataMapping(table, dataMapping.getTargetTable());
				currentMappings.put(dataMapping.getTargetTable(), newMapping);
			}

			com.google.common.collect.Table<String, String, Transformation> newColumnMappings = newMapping.getColumnMappings();
			com.google.common.collect.Table<String, String, Transformation> columnMappings = dataMapping.getColumnMappings();

			com.google.common.collect.Table<String, String, Transformation> currentColumnMappings = currentMapping.getColumnMappings();
			for (Cell<String, String, Transformation> entry : currentColumnMappings.cellSet()) {
				String currentColumnName = entry.getRowKey();
				if (!isFirstDegreeMapping) {
					currentColumnName = entry.getColumnKey();
				}

				Map<String, Transformation> currentColumnMapping = columnMappings.row(currentColumnName);
				if (currentColumnMapping == null) {
					continue;
				}

				for (Entry<String, Transformation> mapping : currentColumnMapping.entrySet()) {
					String newColumnName = mapping.getKey();
					DataMapping.Transformation transformation = mapping.getValue();

					DataMapping.Transformation newTransformation = entry.getValue().apply(transformation);
					newColumnMappings.put(entry.getRowKey(), newColumnName, newTransformation);
				}
			}

			getMappings(targetTable, direction).stream()
					.filter(mapping -> !processed.contains(mapping))
					.forEach(toProcess::add);
		}

		intermediateTables.forEach(currentMappings::remove);
		return Sets.newHashSet(currentMappings.values());
	}

	public List<DataMapping> getMappings(Table table, Direction direction) {
		checkArgument(table != null, "You must specify a 'table'.");
		checkArgument(direction != null, "You must specify a 'direction'.");

		switch (direction) {
			case FORWARDS:
				return Lists.newArrayList(mappings.row(table).values());
			case BACKWARDS:
				return mappings.column(table).values().stream()
						.map(DataMapping::copyAndInverse)
						.collect(Collectors.toList());
			default:
				String supportedDirections = Joiner.on(", ").join(new Direction[] {
						Direction.FORWARDS, Direction.BACKWARDS });

				throw new IllegalArgumentException("Only directions: " + supportedDirections + " are supported!");
		}
	}

}
