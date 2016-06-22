package io.quantumdb.core.backends.postgresql.planner;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.quantumdb.core.backends.postgresql.planner.Graph.GraphResult;
import io.quantumdb.core.backends.postgresql.planner.MigrationState.Progress;
import io.quantumdb.core.backends.postgresql.planner.Operation.Type;
import io.quantumdb.core.backends.postgresql.planner.Plan.Builder;
import io.quantumdb.core.migration.VersionTraverser;
import io.quantumdb.core.migration.operations.SchemaOperationsMigrator;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GreedyMigrationPlanner implements MigrationPlanner {

	private static class ResetException extends RuntimeException {
		public ResetException() {}
	}

	public Plan createPlan(io.quantumdb.core.versioning.State state, Version from, Version to) {
		log.debug("Creating migration plan for migration from version: {} to: {}", from, to);

		Catalog catalog = state.getCatalog();
		RefLog refLog = state.getRefLog();
		SchemaOperationsMigrator migrator = new SchemaOperationsMigrator(catalog, refLog);

		List<Version> migrationPath = VersionTraverser.findChildPath(from, to)
				.orElseThrow(() -> new IllegalStateException("No path from " + from.getId() + " to " + to.getId()));

		migrationPath.remove(from);

		log.debug("Planning the execution of the following operations:");
		for (Version version : migrationPath) {
			if (version.getOperation() instanceof SchemaOperation) {
				log.debug("\t{} - {}", version.getId(), version.getOperation());
			}
			else {
				throw new IllegalArgumentException("Cannot execute data operation during schema operations phase: "
						+ version);
			}
		}

		migrationPath.stream()
				.filter(version -> version.getParent() != null)
				.forEachOrdered(version -> migrator.migrate(version, (SchemaOperation) version.getOperation()));

		Set<String> preTableIds = refLog.getTableRefs(from).stream()
				.map(TableRef::getTableId)
				.collect(Collectors.toSet());

		Set<String> postTableIds = refLog.getTableRefs(to).stream()
				.map(TableRef::getTableId)
				.collect(Collectors.toSet());

		Set<String> newTableIds = Sets.difference(postTableIds, preTableIds);

		log.debug("The following ghost tables will be created: " + newTableIds.stream()
				.collect(Collectors.toMap(Function.identity(), (id) -> refLog.getTableRefById(id).getName())));

		return new Planner(state, from, to, newTableIds, migrator.getRefLog()).createPlan();
	}

	private static class Planner {

		private final Catalog catalog;
		private final Version from;
		private final Version to;
		private final Set<String> newTableIds;
		private final RefLog refLog;

		private Set<String> tableIdsWithNullRecords;
		private Builder plan;
		private MigrationState migrationState;
		private Graph graph;

		public Planner(State state, Version from, Version to, Set<String> newTableIds, RefLog refLog) {
			this.catalog = state.getCatalog();
			this.from = from;
			this.to = to;
			this.newTableIds = Sets.newHashSet(newTableIds);
			this.refLog = refLog;

			this.tableIdsWithNullRecords = Sets.newHashSet();
			this.graph = Graph.fromCatalog(catalog, newTableIds);
			this.migrationState = new MigrationState(catalog);
			this.plan = Plan.builder(migrationState);
		}

		private void reset() {
			this.tableIdsWithNullRecords = Sets.newHashSet();
			this.graph = Graph.fromCatalog(catalog, newTableIds);
			this.migrationState = new MigrationState(catalog);
			this.plan = Plan.builder(migrationState);
		}

		public Plan createPlan() {
			Set<String> toDo;
			while (!(toDo = listToDo()).isEmpty()) {
				try {
					GraphResult least = graph.leastOutgoingForeignKeys(toDo).get();
					if (least.getCount() == 0) {
						migrateTables(least.getTableNames());
					}
					else {
						migrateCoreTables(least.getTableNames());
					}
				}
				catch (ResetException e) {
					reset();
				}
			}
			addDropNullsStep();

			Set<Table> ghostTables = newTableIds.stream()
					.map(catalog::getTable)
					.collect(Collectors.toSet());

			return plan.build(refLog, ghostTables);
		}

		private Set<String> listToDo() {
			return Sets.difference(
					Sets.difference(graph.getTableIds(), migrationState.getPartiallyMigratedTables()),
					migrationState.getMigratedTables());
		}

		private Set<Step> migrateCoreTables(Set<String> tableIds) {
			GraphResult most = graph.mostIncomingForeignKeys(tableIds).get();
			List<String> toMigrate = Lists.newArrayList(most.getTableNames());
			log.debug("Migrating tables: " + toMigrate);

			Set<Step> newSteps = Sets.newHashSet();
			while (!toMigrate.isEmpty()) {
				String tableId = toMigrate.remove(0);
				Table table = catalog.getTable(tableId);
				Set<String> columns = table.getColumns().stream()
						.filter(column -> {
							if (most.getCount() == 0) {
								return true;
							}
							ForeignKey outgoingForeignKey = column.getOutgoingForeignKey();
							if (outgoingForeignKey == null) {
								return true;
							}

							String otherTableId = outgoingForeignKey.getReferredTableName();
							return !graph.getTableIds().contains(otherTableId) ||
									migrationState.getProgress(otherTableId) != Progress.PENDING;
						})
						.map(Column::getName)
						.collect(Collectors.toCollection(Sets::newLinkedHashSet));

				Set<String> identityColumns = table.getIdentityColumns().stream()
						.map(Column::getName)
						.collect(Collectors.toSet());

				SetView<String> missingIdentityColumns = Sets.difference(identityColumns, columns);
				if (!missingIdentityColumns.isEmpty()) {
					toMigrate.add(0, tableId);

					List<Table> parentTables = table.getIdentityColumns().stream()
							.map(Column::getOutgoingForeignKey)
							.map(ForeignKey::getReferredTable)
							.distinct()
							.collect(Collectors.toList());

					parentTables.forEach(parentTable -> {
						toMigrate.remove(parentTable.getName());
						toMigrate.add(0, parentTable.getName());
					});

					continue;
				}

				Set<Table> dependentOnTables = table.getForeignKeys().stream()
						.filter(foreignKey -> !Sets.intersection(Sets.newHashSet(foreignKey.getReferencingColumns()), columns).isEmpty())
						.map(ForeignKey::getReferredTableName)
						.map(catalog::getTable)
						.distinct()
						.collect(Collectors.toSet());

				Set<Step> dependentOnSteps = dependentOnTables.stream()
						.map(plan::findFirstCopy)
						.flatMap(o -> o.isPresent() ? Stream.of(o.get()) : Stream.empty())
						.collect(Collectors.toSet());

				Map<Step, Set<Step>> mapping = dependentOnSteps.stream()
						.collect(Collectors.toMap(Function.identity(), Step::getTransitiveDependencies));

				while (!mapping.isEmpty()) {
					Optional<Entry<Step, Set<Step>>> crucial = mapping.entrySet().stream()
							.sorted(Comparator.comparing(entry -> entry.getValue().size() * -1))
							.findFirst();

					if (crucial.isPresent()) {
						Entry<Step, Set<Step>> entry = crucial.get();
						Step step = entry.getKey();
						dependentOnSteps.removeAll(step.getTransitiveDependencies());
						mapping.remove(step);
					}
					else {
						break;
					}
				}

				Step step = plan.copy(table, columns);
				dependentOnSteps.forEach(step::makeDependentOn);

				applyRules(step);
				newSteps.add(step);
			}
			return newSteps;
		}

		private Set<Step> migrateTables(Set<String> tableIds) {
			List<String> toMigrate = Lists.newArrayList(tableIds);
			log.debug("Migrating tables: " + toMigrate);


			Set<Step> newSteps = Sets.newHashSet();
			while (!toMigrate.isEmpty()) {
				String tableId = toMigrate.remove(0);

				Table table = catalog.getTable(tableId);
				Set<String> columns = table.getColumns().stream()
						.filter(column -> {
							ForeignKey outgoingForeignKey = column.getOutgoingForeignKey();
							if (outgoingForeignKey == null) {
								return true;
							}

							String otherTableId = outgoingForeignKey.getReferredTableName();
							return !graph.getTableIds().contains(otherTableId) ||
									migrationState.getProgress(otherTableId) != Progress.PENDING;
						})
						.map(Column::getName)
						.collect(Collectors.toCollection(Sets::newLinkedHashSet));

				Set<String> identityColumns = table.getIdentityColumns().stream()
						.map(Column::getName)
						.collect(Collectors.toSet());

				SetView<String> missingIdentityColumns = Sets.difference(identityColumns, columns);
				if (!missingIdentityColumns.isEmpty()) {
					toMigrate.add(0, tableId);

					List<Table> parentTables = table.getIdentityColumns().stream()
							.map(Column::getOutgoingForeignKey)
							.map(ForeignKey::getReferredTable)
							.distinct()
							.collect(Collectors.toList());

					parentTables.forEach(parentTable -> {
						toMigrate.remove(parentTable.getName());
						toMigrate.add(0, parentTable.getName());
					});

					continue;
				}

				Set<Table> dependentOnTables = table.getForeignKeys().stream()
						.filter(foreignKey -> !Sets.intersection(Sets.newHashSet(foreignKey.getReferencingColumns()), columns).isEmpty())
						.map(ForeignKey::getReferredTableName)
						.map(catalog::getTable)
						.distinct()
						.collect(Collectors.toSet());

				Set<Step> dependentOnSteps = dependentOnTables.stream()
						.map(plan::findFirstCopy)
						.flatMap(o -> o.isPresent() ? Stream.of(o.get()) : Stream.empty())
						.collect(Collectors.toSet());

				Map<Step, Set<Step>> mapping = dependentOnSteps.stream()
						.collect(Collectors.toMap(Function.identity(), Step::getTransitiveDependencies));

				while (!mapping.isEmpty()) {
					Optional<Entry<Step, Set<Step>>> crucial = mapping.entrySet().stream()
							.sorted(Comparator.comparing(entry -> entry.getValue().size() * -1))
							.findFirst();

					if (crucial.isPresent()) {
						Entry<Step, Set<Step>> entry = crucial.get();
						Step step = entry.getKey();
						dependentOnSteps.removeAll(step.getTransitiveDependencies());
						mapping.remove(step);
					}
					else {
						break;
					}
				}

				Step step = plan.copy(table, columns);
				dependentOnSteps.forEach(step::makeDependentOn);

				applyRules(step);
				newSteps.add(step);
			}
			return newSteps;
		}

		private void addDropNullsStep() {
			Set<Table> tables = tableIdsWithNullRecords.stream()
					.map(catalog::getTable)
					.collect(Collectors.toSet());

			if (tables.isEmpty()) {
				return;
			}

			Set<Step> stepsWithNoDependees = Sets.newHashSet(plan.getSteps());
			plan.getSteps().forEach(step -> step.getDependencies().forEach(stepsWithNoDependees::remove));

			Step step = plan.dropNullRecord(tables);
			stepsWithNoDependees.forEach(step::makeDependentOn);
		}

		private void applyRules(Step step) {
			applyDependencyRule(step);
			applyCompletionRule(step);
		}

		private void applyDependencyRule(Step step) {
			Operation operation = step.getOperation();
			Type operationType = operation.getType();
			if (operationType != Type.COPY && operationType != Type.ADD_NULL) {
				return;
			}

			Set<Table> tables = operation.getTables();
			for (Table table : tables) {
				Set<String> copiedColumns = table.getColumns().stream()
						.map(Column::getName)
						.collect(Collectors.toSet());

				if (operationType == Type.COPY && (operation.getColumns().containsAll(copiedColumns))) {
					return;
				}

				for (ForeignKey foreignKey : table.getForeignKeys()) {
					if (operationType == Type.COPY) {
						if (!foreignKey.isNotNullable() && !foreignKey.isInheritanceRelation()) {
							continue;
						}
					}
					else if (operationType == Type.ADD_NULL) {
						if (!foreignKey.isNotNullable() && !foreignKey.isInheritanceRelation()) {
							continue;
						}
					}

					Table otherTable = foreignKey.getReferredTable();
					if (!tableIdsWithNullRecords.contains(otherTable.getName())) {
						TableNode tableNode = graph.get(otherTable.getName());
						if (tableNode == null) {
							expand(Sets.newHashSet(otherTable.getName()));
							throw new ResetException();
						}
						else {
							tableIdsWithNullRecords.add(otherTable.getName());
							if (operationType == Type.ADD_NULL) {
								step.getOperation().addTable(otherTable);
								applyDependencyRule(step);
							}
							else {
								Step dependency = plan.addNullRecord(Sets.newHashSet(otherTable));
								step.makeDependentOn(dependency);
								applyDependencyRule(dependency);
							}
						}
					}
				}
			}
		}

		private void applyCompletionRule(Step step) {
			Operation operation = step.getOperation();
			if (operation.getType() != Type.COPY) {
				return;
			}

			for (String tableName : migrationState.getPartiallyMigratedTables()) {
				Set<String> toMigrate = migrationState.getYetToBeMigratedColumns(tableName);
				if (toMigrate.isEmpty()) {
					continue;
				}

				Table table = catalog.getTable(tableName);
				boolean allDependenciesMigrated = table.getForeignKeys().stream()
						.allMatch(foreignKey -> {
							String otherTableId = foreignKey.getReferredTableName();
							return !graph.getTableIds().contains(otherTableId) ||
									migrationState.getProgress(otherTableId) != Progress.PENDING;
						});

				if (allDependenciesMigrated) {
					Set<Table> dependentOnTables = table.getForeignKeys().stream()
							.filter(foreignKey -> !Sets.intersection(Sets.newHashSet(foreignKey.getReferencingColumns()), toMigrate).isEmpty())
							.map(ForeignKey::getReferredTableName)
							.map(catalog::getTable)
							.distinct()
							.collect(Collectors.toSet());

					Set<Step> dependentOnSteps = dependentOnTables.stream()
							.map(plan::findFirstCopy)
							.flatMap(o -> o.isPresent() ? Stream.of(o.get()) : Stream.empty())
							.collect(Collectors.toSet());

					Map<Step, Set<Step>> mapping = dependentOnSteps.stream()
							.collect(Collectors.toMap(Function.identity(), Step::getTransitiveDependencies));

					while (!mapping.isEmpty()) {
						Optional<Entry<Step, Set<Step>>> crucial = mapping.entrySet().stream()
								.sorted(Comparator.comparing(entry -> entry.getValue().size() * -1))
								.findFirst();

						if (crucial.isPresent()) {
							Entry<Step, Set<Step>> entry = crucial.get();
							Step key = entry.getKey();
							dependentOnSteps.removeAll(key.getTransitiveDependencies());
							mapping.remove(key);
						}
						else {
							break;
						}
					}

					Step dependent = plan.copy(table, toMigrate);
					dependentOnSteps.forEach(dependent::makeDependentOn);
					applyRules(dependent);
				}
			}
		}

		private void expand(Set<String> tableIdsToExpand) {
			log.trace("Creating ghost tables for: " + tableIdsToExpand);

			List<String> tableIdsToMirror = Lists.newArrayList(tableIdsToExpand);
			Multimap<TableRef, TableRef> ghostedTableIds = refLog.getTableMapping(from, to, true);
			Set<String> createdGhostTableIds = Sets.newHashSet();

			while(!tableIdsToMirror.isEmpty()) {
				String tableId = tableIdsToMirror.remove(0);
				if (ghostedTableIds.entries().stream()
						.anyMatch(entry -> entry.getKey().getTableId().equals(tableId))) {
					continue;
				}

				TableRef tableRef = refLog.getTableRefById(tableId);
				Table table = catalog.getTable(tableRef.getTableId());

				String newTableId = RandomHasher.generateTableId(refLog);
				TableRef ghostTableRef = tableRef.ghost(newTableId, to);

				Table ghostTable = table.copy().rename(ghostTableRef.getTableId());
				catalog.addTable(ghostTable);

				createdGhostTableIds.add(ghostTableRef.getTableId());
				ghostedTableIds.put(tableRef, ghostTableRef);

				log.debug("Planned creation of ghost table: {} for source table: {}", newTableId, tableRef.getName());

				// Traverse incoming foreign keys
				TableRef oldTableRef = refLog.getTableRef(from, tableRef.getName());
				Set<String> tableIdsAtOrigin = refLog.getTableRefs(from).stream()
						.map(TableRef::getTableId)
						.collect(Collectors.toSet());

				catalog.getTablesReferencingTable(oldTableRef.getTableId()).stream()
						.filter(tableIdsAtOrigin::contains)
						.filter(referencingTableId -> !ghostedTableIds.containsKey(referencingTableId)
								&& !tableIdsToMirror.contains(referencingTableId))
						.distinct()
						.forEach(tableIdsToMirror::add);
			}

			// Copying foreign keys for each affected table.
			for (Entry<TableRef, TableRef> entry : ghostedTableIds.entries()) {
				TableRef oldTableRef = entry.getKey();
				TableRef newTableRef = entry.getValue();

				Table oldTable = catalog.getTable(oldTableRef.getTableId());
				Table newTable = catalog.getTable(newTableRef.getTableId());

				if (newTableIds.contains(newTableRef.getTableId())) {
					List<ForeignKey> foreignKeysToFix = newTable.getForeignKeys().stream()
							.filter(fk -> {
								String referredTableId = fk.getReferredTableName();
								Set<String> tableIdsAsTarget = refLog.getTableRefs(to).stream()
										.map(TableRef::getTableId)
										.collect(Collectors.toSet());

								return !tableIdsAsTarget.contains(referredTableId);
							})
							.collect(Collectors.toList());

					foreignKeysToFix.forEach(fk -> {
						fk.drop();
						String referredTableId = fk.getReferredTableName();
						TableRef referredTableRef = refLog.getTableRefById(referredTableId);
						TableRef mappedTableRef = refLog.getTableRef(to, referredTableRef.getName());
						Table referredTable = catalog.getTable(mappedTableRef.getTableId());

						newTable.addForeignKey(fk.getReferencingColumns())
								.named(fk.getForeignKeyName())
								.onUpdate(fk.getOnUpdate())
								.onDelete(fk.getOnDelete())
								.referencing(referredTable, fk.getReferredColumns());
					});
				}
				else {
					List<ForeignKey> outgoingForeignKeys = Lists.newArrayList(oldTable.getForeignKeys());
					for (ForeignKey foreignKey : outgoingForeignKeys) {
						String oldReferredTableId = foreignKey.getReferredTableName();
						TableRef oldReferredTableRef = refLog.getTableRefById(oldReferredTableId);
						TableRef newReferredTableRef = refLog.getTableRef(to, oldReferredTableRef.getName());

						Table newReferredTable = catalog.getTable(newReferredTableRef.getTableId());
						newTable.addForeignKey(foreignKey.getReferencingColumns())
								.named(foreignKey.getForeignKeyName())
								.onUpdate(foreignKey.getOnUpdate())
								.onDelete(foreignKey.getOnDelete())
								.referencing(newReferredTable, foreignKey.getReferredColumns());
					}
				}
			}

			newTableIds.addAll(createdGhostTableIds);
		}
	}

}
