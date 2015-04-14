package io.quantumdb.core.backends.postgresql.planner;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.quantumdb.core.migration.operations.SchemaOperationsMigrator;
import io.quantumdb.core.migration.utils.DataMapping.Transformation;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.migration.utils.VersionTraverser;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
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
		Set<String> newTableIds = Sets.difference(postTableIds, preTableIds);

		log.debug("The following new tables will be created: " + newTableIds.stream()
				.collect(Collectors.toMap(Function.identity(), (id) -> tableMapping.getTableName(to, id))));

		return determinePlan(catalog, tableMapping, from, to, newTableIds, migrator.getDataMappings());
	}

	private MigrationPlan determinePlan(Catalog catalog, TableMapping tableMapping, Version sourceVersion,
			Version targetVersion, Set<String> newTableIds, DataMappings mappings) {

		Set<String> originalModifiedTableIds = Sets.newHashSet(newTableIds);
		Map<String, TableNode> graph = constructGraph(catalog, newTableIds);
		List<String> tableIdsWithNullRecords = Lists.newArrayList();
		List<Step> steps = Lists.newArrayList();
		boolean lookForCheapMigrations = true;

		while (!graph.isEmpty()) {
			if (lookForCheapMigrations) {
				Map<String, Integer> selectedTables = selectTablesWithLowestNumberOfNonNullableIncomingForeignKeys(graph);
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
					node.getForeignKeys().forEach(foreignKey -> {
						columnNames.addAll(foreignKey.getReferencingColumns());
						if (!tableIdsWithNullRecords.contains(foreignKey.getReferredTableName())) {
							tableIdsWithNullRecords.add(foreignKey.getReferredTableName());
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

				Set<String> tableIdsToAdd = tablesToAdd.stream()
						.map(tableName -> tableMapping.getTableId(targetVersion, tableName))
						.collect(Collectors.toSet());

				Set<String> newGhostTableIds = expand(catalog, tableMapping, mappings, sourceVersion, targetVersion, /* newTableIds, */ tableIdsToAdd, originalModifiedTableIds);

				lookForCheapMigrations = true;
				tableIdsWithNullRecords.clear();
				steps.clear();

				newTableIds = Sets.union(newTableIds, newGhostTableIds);

				newGhostTableIds.forEach(newGhostTableId -> {
					if (!tableIdsWithNullRecords.contains(newGhostTableId)) {
						tableIdsWithNullRecords.add(newGhostTableId);
					}
				});

				graph.clear();
				graph.putAll(constructGraph(catalog, newTableIds));
			}
		}

		return new MigrationPlan(Lists.reverse(steps), tableIdsWithNullRecords, mappings);
	}

	private Set<String> expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings,
			Version sourceVersion, Version targetVersion, /* Map<String, String> createdGhostTableIds, */
			Set<String> tableIdsToExpand,
			Set<String> originalModifiedTables) {

		List<String> tableIdsToMirror = Lists.newArrayList(tableIdsToExpand);
		Multimap<String, String> ghostedTableIds = tableMapping.getGhostTableIdMapping(sourceVersion, targetVersion);
		Set<String> createdGhostTableIds = Sets.newHashSet();

		while(!tableIdsToMirror.isEmpty()) {
			String tableId = tableIdsToMirror.remove(0);
			if (ghostedTableIds.containsKey(tableId)) {
				continue;
			}

			String tableName = tableMapping.getTableName(targetVersion, tableId);
			Table table = catalog.getTable(tableId);

			String newTableId = RandomHasher.generateTableId(tableMapping);
			tableMapping.ghost(targetVersion, tableName, newTableId);
			Table ghostTable = table.copy().rename(newTableId);
			catalog.addTable(ghostTable);

			for (Column column : table.getColumns()) {
				dataMappings.drop(table, column.getName());
				dataMappings.add(table, column.getName(), ghostTable, column.getName(), Transformation.createNop());
			}

			createdGhostTableIds.add(newTableId);
			ghostedTableIds.put(tableId, newTableId);

			log.debug("Planned creation of ghost table: {} for source table: {}", newTableId, tableName);

			// Traverse incoming foreign keys
			String oldTableId = tableMapping.getTableId(sourceVersion, tableName);
			catalog.getTablesReferencingTable(oldTableId).stream()
					.filter(tableMapping.getTableIds(sourceVersion)::contains)
					.filter(referencingTableId -> !ghostedTableIds.containsKey(referencingTableId)
							&& !tableIdsToMirror.contains(referencingTableId))
					.distinct()
					.forEach(tableIdsToMirror::add);

			// Traverse outgoing non-nullable foreign keys
			table.getForeignKeys().stream()
					.filter(ForeignKey::containsNonNullableColumns)
					.map(ForeignKey::getReferredTableName)
					.distinct()
					.filter(referencedTableId -> !ghostedTableIds.containsKey(referencedTableId)
							&& !tableIdsToMirror.contains(referencedTableId))
					.forEach(tableIdsToMirror::add);
		}

		// Copying foreign keys for each affected table.
		for (Entry<String, String> entry : ghostedTableIds.entries()) {
			String oldTableId = entry.getKey();
			String newTableId = entry.getValue();

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

		return createdGhostTableIds;
	}

	private static Map<String, TableNode> constructGraph(Catalog catalog, Set<String> tableNames) {
		Map<String, TableNode> nodes = Maps.newHashMap();
		for (String tableName : tableNames) {
			nodes.put(tableName, new TableNode(tableName, Lists.newArrayList()));
		}

		for (String tableName : tableNames) {
			TableNode node = nodes.get(tableName);

			Table table = catalog.getTable(tableName);
			table.getForeignKeys().forEach(foreignKey -> {
				String referredTableName = foreignKey.getReferredTableName();
				if (tableNames.contains(referredTableName)) {
					node.getForeignKeys().add(foreignKey);
				}
			});
		}

		return nodes;
	}

	private static void removeTableFromGraph(Map<String, TableNode> graph, String tableName) {
		TableNode removed = graph.remove(tableName);
		if (removed != null) {
			graph.values().forEach(node -> {
				Set<ForeignKey> toRemoved = Sets.newHashSet();
				node.getForeignKeys().forEach(foreignKey -> {
					if (tableName.equals(foreignKey.getReferredTableName())) {
						toRemoved.add(foreignKey);
					}
				});

				toRemoved.forEach(node.getForeignKeys()::remove);
			});
		}
	}

	private static Map<String, Integer> selectTablesWithLowestNumberOfNonNullableIncomingForeignKeys(Map<String, TableNode> graph) {
		Set<String> tableNames = Sets.newHashSet();
		int lowestAmountOfIncomingForeignKeys = Integer.MAX_VALUE;

		for (TableNode node : graph.values()) {
			int incomingForeignKeys = countIncomingNonNullableForeignKeys(graph, node);
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
				.collect(Collectors.toMap(TableNode::getTableName, node -> countIncomingNonNullableForeignKeys(graph, node)));
	}

	private static int countIncomingNonNullableForeignKeys(Map<String, TableNode> graph, TableNode target) {
		Set<Table> referencingTables = Sets.newHashSet();
		for (TableNode node : graph.values()) {
			List<ForeignKey> foreignKeys = node.getForeignKeys();
			boolean referencesTargetTable = foreignKeys.stream()
					.map(ForeignKey::getReferredTableName)
					.anyMatch(target.getTableName()::equals);

			if (!referencesTargetTable) {
				continue;
			}

			for (ForeignKey foreignKey : foreignKeys) {
				Table referencingTable = foreignKey.getReferencingTable();
				boolean notNullable = foreignKey.getReferencingColumns().stream()
					.map(referencingTable::getColumn)
					.anyMatch(Column::isNotNull);

				if (notNullable) {
					referencingTables.add(referencingTable);
				}
			}
		}

		return referencingTables.size();
	}

}
