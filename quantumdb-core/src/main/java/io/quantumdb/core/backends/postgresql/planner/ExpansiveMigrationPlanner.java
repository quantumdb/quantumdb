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
import com.google.common.collect.Sets.SetView;
import io.quantumdb.core.migration.operations.SchemaOperationsMigrator;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.migration.utils.VersionTraverser;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExpansiveMigrationPlanner implements MigrationPlanner {

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

		log.debug("The following new tables will be created: " + newTables.stream().collect(Collectors.toMap(Function.identity(), (id) -> tableMapping.getTableName(to, id))));

		return determinePlan(catalog, tableMapping, from, to, newTables, migrator.getDataMappings());
	}

	private MigrationPlan determinePlan(Catalog catalog, TableMapping tableMapping, Version sourceVersion,
			Version targetVersion, Set<String> tableNames, DataMappings mappings) {

		Set<String> originalModifiedTables = Sets.newHashSet(tableNames);
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
					node.getForeignKeys().forEach((columnNamesList, targetNode) -> {
						columnNames.addAll(columnNamesList);
						if (!tablesToCreateNullObjectsFor.contains(targetNode.getTableName())) {
							tablesToCreateNullObjectsFor.add(targetNode.getTableName());
						}
					});
					migrations.add(new Migration(tableName, columnNames));
				});
				steps.add(new Step(migrations));

				Set<String> tablesWhichMustBePresentInPlan = migrations.stream()
						.map(Migration::getTableName)
						.map(catalog::getTable)
						.flatMap(table -> table.getForeignKeys().stream())
						.filter(ForeignKey::containsNonNullableColumns)
						.map(ForeignKey::getReferredTableName)
						.map(tableId -> tableMapping.getTableName(tableId).get())
						.distinct()
						.collect(Collectors.toSet());

				Set<String> tablesInPlan = steps.stream()
						.flatMap(step -> step.getTableMigrations().stream())
						.map(Migration::getTableName)
						.map(tableId -> tableMapping.getTableName(targetVersion, tableId))
						.distinct()
						.collect(Collectors.toSet());

				SetView<String> tablesToAdd = Sets.difference(tablesWhichMustBePresentInPlan, tablesInPlan);
				if (tablesToAdd.isEmpty()) {
					break;
				}

				Set<String> newGhostTables = expand(catalog, tableMapping, sourceVersion, targetVersion, tableNames, tablesToAdd, originalModifiedTables);

				lookForCheapMigrations = true;
				tablesToCreateNullObjectsFor.clear();
				steps.clear();

				tableNames = Sets.union(tableNames, newGhostTables.stream()
						.map(tableName -> tableMapping.getTableId(targetVersion, tableName))
						.collect(Collectors.toSet()));

				newGhostTables.forEach(newGhostTable -> {
					if (!tablesToCreateNullObjectsFor.contains(newGhostTable)) {
						tablesToCreateNullObjectsFor.add(newGhostTable);
					}
				});

				graph.clear();
				graph.putAll(constructGraph(catalog, tableNames));
			}
		}

		return new MigrationPlan(Lists.reverse(steps), tablesToCreateNullObjectsFor, mappings);
	}

	private Set<String> expand(Catalog catalog, TableMapping tableMapping, Version sourceVersion,
			Version targetVersion, Set<String> alreadyExpanded, Set<String> tablesToExpand,
			Set<String> originalModifiedTables) {

		Set<String> createdGhostTables = Sets.newHashSet();
		Set<String> mirrored = alreadyExpanded.stream()
				.map(tableId -> tableMapping.getTableName(targetVersion, tableId))
				.collect(Collectors.toSet());

		List<String> tablesToMirror = Lists.newArrayList(tablesToExpand);

		while(!tablesToMirror.isEmpty()) {
			String tableName = tablesToMirror.remove(0);
			String tableId = tableMapping.getTableId(targetVersion, tableName);
			if (mirrored.contains(tableName)) {
				continue;
			}

			Table table = catalog.getTable(tableId);

			String newTableId = RandomHasher.generateTableId(tableMapping);
			tableMapping.set(targetVersion, tableName, newTableId);
			catalog.addTable(table.copy()
					.rename(newTableId));

			mirrored.add(tableName);
			createdGhostTables.add(tableName);

			log.debug("Planned creation of ghost table: {} for source table: {}", newTableId, tableName);

			// Traverse incoming foreign keys
			String oldTableId = tableMapping.getTableId(sourceVersion, tableName);
			catalog.getTablesReferencingTable(oldTableId).stream()
					.map(referencingTableId -> tableMapping.getTableName(referencingTableId).get())
					.filter(referencingTableName -> !mirrored.contains(referencingTableName))
					.distinct()
					.forEach(tablesToMirror::add);

			// Traverse outgoing non-nullable foreign keys
			table.getForeignKeys().stream()
					.filter(ForeignKey::containsNonNullableColumns)
					.map(ForeignKey::getReferredTableName)
					.distinct()
					.filter(referencedTableName -> !tablesToMirror.contains(referencedTableName))
					.forEach(tablesToMirror::add);
		}

		// Copying foreign keys for each affected table.
		for (String tableName : mirrored) {
			String oldTableId = tableMapping.getTableId(sourceVersion, tableName);
			String newTableId = tableMapping.getTableId(targetVersion, tableName);

			Table oldTable = catalog.getTable(oldTableId);
			Table newTable = catalog.getTable(newTableId);

			if (originalModifiedTables.contains(newTableId)) {
				List<ForeignKey> foreignKeysToFix = newTable.getForeignKeys().stream()
						.filter(fk -> {
							String referredTableId = fk.getReferredTableName();
							Map<String, String> version = tableMapping.getTableMapping(targetVersion);
							return !version.containsKey(referredTableId);
						})
						.collect(Collectors.toList());

				foreignKeysToFix.forEach(fk -> {
					fk.drop();
					String referredTableId = fk.getReferredTableName();
					String referredTableName = tableMapping.getTableName(sourceVersion, referredTableId);
					String mappedTableId = tableMapping.getTableId(targetVersion, referredTableName);
					Table referredTable = catalog.getTable(mappedTableId);
					newTable.addForeignKey(fk.getReferencingColumns()).referencing(referredTable, fk.getReferredColumns());
				});
			}
			else {
				List<ForeignKey> outgoingForeignKeys = oldTable.getForeignKeys();
				for (ForeignKey foreignKey : outgoingForeignKeys) {
					String oldReferredTableId = foreignKey.getReferredTableName();
					String oldReferredTableName = tableMapping.getTableName(sourceVersion, oldReferredTableId);
					String newReferredTableId = tableMapping.getTableId(targetVersion, oldReferredTableName);

					Table newReferredTable = catalog.getTable(newReferredTableId);
					newTable.addForeignKey(foreignKey.getReferencingColumns())
							.referencing(newReferredTable, foreignKey.getReferredColumns());
				}
			}
		}

		return createdGhostTables;
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
