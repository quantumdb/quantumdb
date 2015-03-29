package io.quantumdb.core.backends.postgresql.planner;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.quantumdb.core.migration.operations.SchemaOperationsMigrator;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.migration.utils.VersionTraverser;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

public class AdaptiveMigrationPlanner implements MigrationPlanner {

	public MigrationPlan createPlan(State state, Version from, Version to) {
		Catalog catalog = state.getCatalog();
		TableMapping tableMapping = state.getTableMapping();
		SchemaOperationsMigrator migrator = new SchemaOperationsMigrator(catalog, tableMapping);

		List<Version> migrationPath = VersionTraverser.findChildPath(from, to)
				.orElseThrow(() -> new IllegalStateException("No path from " + from.getId() + " to " + to.getId()));

		migrationPath.remove(from);
		migrationPath.stream()
				.filter(version -> version.getParent() != null)
				.forEachOrdered(version -> migrator.expand(version, version.getSchemaOperation()));

		Set<String> preTableIds = tableMapping.getTableIds(from);
		Set<String> postTableIds = tableMapping.getTableIds(to);
		Set<String> newTables = Sets.difference(postTableIds, preTableIds);

		return determinePlan(catalog, newTables, migrator.getDataMappings());
	}

	private MigrationPlan determinePlan(Catalog catalog, Set<String> tableNames, DataMappings mappings) {
		Map<String, TableNode> graph = constructGraph(catalog, tableNames);
		List<String> tablesToCreateNullObjectsFor = Lists.newArrayList();
		List<Step> steps = Lists.newArrayList();

		boolean lookForCheapMigrations = true;
		while (!graph.isEmpty()) {
			if (lookForCheapMigrations) {
				Map<String, Integer> selectedTables = selectTablesWithLowestNumberOfIncomingForeignKeys(graph);
				Set<Migration> migrations = Sets.newHashSet();
				Set<String> tablesToRemoveFromGraph = Sets.newHashSet();
				for (Map.Entry<String, Integer> entry : selectedTables.entrySet()) {
					String tableName = entry.getKey();
					int incomingForeignKeys = entry.getValue();
					if (incomingForeignKeys == 0) {
						migrations.add(new Migration(tableName, ImmutableSet.of()));
						tablesToRemoveFromGraph.add(tableName);
					}
					else {
						lookForCheapMigrations = false;
						break;
					}
				}
				tablesToRemoveFromGraph.forEach(tableName -> removeTableFromGraph(graph, tableName));
				if (lookForCheapMigrations) {
					steps.add(new Step(migrations));
				}
			}
			else {
				Set<Migration> migrations = Sets.newHashSet();
				selectAllTables(graph).forEach((tableName, incomingForeignKeys) -> {
					TableNode node = graph.get(tableName);
					Set<String> columnNames = Sets.newHashSet();
					node.getForeignKeys().forEach((columnNamesArray, targetNode) -> {
						for (String columnName : columnNamesArray) {
							columnNames.add(columnName);
						}
						if (!tablesToCreateNullObjectsFor.contains(targetNode.getTableName())) {
							tablesToCreateNullObjectsFor.add(targetNode.getTableName());
						}
					});
					migrations.add(new Migration(tableName, columnNames));
				});
				steps.add(new Step(migrations));
				graph.clear();
			}
		}

		return new MigrationPlan(Lists.reverse(steps), tablesToCreateNullObjectsFor, mappings);
	}

	private static Map<String, TableNode> constructGraph(Catalog catalog, Set<String> tableNames) {
		Map<String, TableNode> nodes = Maps.newHashMap();
		for (String tableName : tableNames) {
			nodes.put(tableName, new TableNode(tableName, Maps.newHashMap()));
		}

		for (String tableName : tableNames) {
			TableNode node = nodes.get(tableName);

			Table table = catalog.getTable(tableName);
			table.getForeignKeys().forEach(foreignKey -> {
				String referredTableName = foreignKey.getReferredTableName();
				TableNode referredNode = nodes.get(referredTableName);

				if (tableNames.contains(referredTableName)) {
					node.getForeignKeys().put(foreignKey.getReferencingColumns(), referredNode);
				}
			});
		}

		return nodes;
	}

	private static void removeTableFromGraph(Map<String, TableNode> graph, String tableName) {
		TableNode removed = graph.remove(tableName);
		if (removed != null) {
			graph.values().forEach(node -> {
				Set<List<String>> toRemoved = Sets.newHashSet();
				node.getForeignKeys().forEach((key, value) -> {
					if (tableName.equals(value.getTableName())) {
						toRemoved.add(key);
					}
				});

				toRemoved.forEach(node.getForeignKeys()::remove);
			});
		}
	}

	private static Map<String, Integer> selectTablesWithLowestNumberOfIncomingForeignKeys(Map<String, TableNode> graph) {
		Set<String> tableNames = Sets.newHashSet();
		int lowestAmountOfIncomingForeignKeys = Integer.MAX_VALUE;

		for (TableNode node : graph.values()) {
			int incomingForeignKeys = countIncomingForeignKeys(graph, node);
			if (incomingForeignKeys < lowestAmountOfIncomingForeignKeys) {
				tableNames.clear();
				tableNames.add(node.getTableName());
				lowestAmountOfIncomingForeignKeys = incomingForeignKeys;
			}
			else if (incomingForeignKeys == lowestAmountOfIncomingForeignKeys) {
				tableNames.add(node.getTableName());
			}
		}

		final int incomingForeignKeys = lowestAmountOfIncomingForeignKeys;
		return tableNames.stream()
				.collect(Collectors.toMap(Function.identity(), tableName -> incomingForeignKeys));
	}

	private static Map<String, Integer> selectAllTables(Map<String, TableNode> graph) {
		return graph.values().stream()
				.collect(Collectors.toMap(TableNode::getTableName, node -> countIncomingForeignKeys(graph, node)));
	}

	private static int countIncomingForeignKeys(Map<String, TableNode> graph, TableNode target) {
		int counter = 0;
		for (TableNode node : graph.values()) {
			if (node.getForeignKeys().containsValue(target)) {
				counter++;
			}
		}

		return counter;
	}

}
