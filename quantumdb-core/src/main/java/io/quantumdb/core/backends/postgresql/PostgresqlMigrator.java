package io.quantumdb.core.backends.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;
import io.quantumdb.core.backends.DatabaseMigrator;
import io.quantumdb.core.backends.postgresql.migrator.NullRecords;
import io.quantumdb.core.backends.postgresql.migrator.TableCreator;
import io.quantumdb.core.backends.postgresql.planner.GreedyMigrationPlanner;
import io.quantumdb.core.backends.postgresql.planner.Operation;
import io.quantumdb.core.backends.postgresql.planner.Plan;
import io.quantumdb.core.backends.postgresql.planner.PlanValidator;
import io.quantumdb.core.backends.postgresql.planner.Step;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.state.RefLog;
import io.quantumdb.core.state.RefLog.TableRef;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.versioning.MigrationFunctions;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class PostgresqlMigrator implements DatabaseMigrator {

	private final PostgresqlBackend backend;

	PostgresqlMigrator(PostgresqlBackend backend) {
		this.backend = backend;
	}

	@Override
	public void migrate(State state, Version from, Version to) throws MigrationException {
		RefLog refLog = state.getRefLog();
		Set<Version> preMigration = refLog.getVersions();
		Plan plan = new GreedyMigrationPlanner().createPlan(state, from, to);

		PlanValidator.validate(plan);
		Set<Version> postMigration = refLog.getVersions();
		Set<Version> intermediateVersions = Sets.newHashSet(Sets.difference(postMigration, preMigration));
		intermediateVersions.remove(to);

		new InternalPlanner(backend, plan, state, from, to, intermediateVersions).migrate();
	}

	@Override
	public void drop(State state, Version version) throws MigrationException {
		RefLog refLog = state.getRefLog();
		List<TableRef> tablesToDrop = refLog.getTableRefs().stream()
				.filter(tableRef -> tableRef.getVersions().contains(version))
				.filter(tableRef -> tableRef.getVersions().size() == 1)
				.collect(Collectors.toList());

		log.info("Determined the following tables will be dropped: {}", tablesToDrop);
		try (Connection connection = backend.connect()) {
			dropTriggers(connection, state.getFunctions(), tablesToDrop);
			dropFunctions(connection, state.getFunctions(), tablesToDrop);
			dropTables(connection, refLog, tablesToDrop);
			backend.persistState(state);
		}
		catch (SQLException e) {
			throw new MigrationException(e);
		}
	}

	private void dropTriggers(Connection connection, MigrationFunctions migrationFunctions,
			List<TableRef> tablesToDrop) throws SQLException {

		for (TableRef table : tablesToDrop) {
			String tableId = table.getTableId();

			com.google.common.collect.Table<String, String, String> triggers = migrationFunctions.getTriggers(tableId);
			for (Cell<String, String, String> function : triggers.cellSet()) {
				String triggerName = function.getValue();
				String sourceTableId = function.getRowKey();
				String targetTableId = function.getColumnKey();
				try (Statement statement = connection.createStatement()) {
					statement.execute("DROP TRIGGER " + triggerName + " ON " + sourceTableId + ";");
				}
				migrationFunctions.removeTrigger(sourceTableId, targetTableId);
			}
		}
	}

	private void dropFunctions(Connection connection, MigrationFunctions migrationFunctions,
			List<TableRef> tablesToDrop) throws SQLException {

		for (TableRef table : tablesToDrop) {
			String tableId = table.getTableId();

			com.google.common.collect.Table<String, String, String> functions = migrationFunctions.getFunctions(tableId);
			for (Cell<String, String, String> function : functions.cellSet()) {
				String functionName = function.getValue();
				String sourceTableId = function.getRowKey();
				String targetTableId = function.getColumnKey();
				try (Statement statement = connection.createStatement()) {
					statement.execute("DROP FUNCTION " + functionName + "();");
				}
				migrationFunctions.removeFunction(sourceTableId, targetTableId);
			}
		}
	}

	private void dropTables(Connection connection, RefLog refLog, List<TableRef> tablesToDrop) throws SQLException {
		for (TableRef table : tablesToDrop) {
			String tableId = table.getTableId();
			try (Statement statement = connection.createStatement()) {
				statement.execute("DROP TABLE " + tableId + " CASCADE;");
			}

			refLog.dropTable(table);
		}
	}

	static class InternalPlanner {

		private final Plan plan;
		private final Set<Version> intermediateVersions;
		private final RefLog refLog;
		private final State state;
		private final NullRecords nullRecords;
		private final Multimap<Table, String> migratedColumns;
		private final PostgresqlBackend backend;
		private final Version from;
		private final Version to;

		private final com.google.common.collect.Table<String, String, SyncFunction> syncFunctions;


		public InternalPlanner(PostgresqlBackend backend, Plan plan, State state, Version from, Version to,
				Set<Version> intermediateVersions) {

			this.backend = backend;
			this.plan = plan;
			this.intermediateVersions = intermediateVersions;
			this.refLog = plan.getRefLog();
			this.state = state;
			this.nullRecords = new NullRecords();
			this.migratedColumns = HashMultimap.create();
			this.syncFunctions = HashBasedTable.create();
			this.from = from;
			this.to = to;
		}

		public void migrate() throws MigrationException {
			createGhostTables();

			Optional<Step> nextStep;
			while ((nextStep = plan.nextStep()).isPresent()) {
				try {
					Step step = nextStep.get();
					execute(step.getOperation());
					step.markAsExecuted();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new MigrationException(e);
				}
			}

			createIndexes();

			synchronizeBackwards();

//			intermediateVersions.forEach(state.getTableMapping()::remove);

			persistState();
		}

		private void persistState() throws MigrationException {
			try {
				backend.persistState(state);
			}
			catch (SQLException e) {
				throw new MigrationException(e);
			}
		}

		private void execute(Operation operation) throws MigrationException, InterruptedException {

			log.info("Executing operation: " + operation);
			try {
				Set<Table> tables = operation.getTables();
				switch (operation.getType()) {
					case ADD_NULL:
						nullRecords.insertNullObjects(backend, tables);
						break;
					case DROP_NULL:
						nullRecords.deleteNullObjects(backend, tables);
						break;
					case COPY:
						Table table = tables.iterator().next();
						Set<String> columns = operation.getColumns();
						Set<String> previouslyMigrated = Sets.newHashSet(this.migratedColumns.get(table));
						Set<String> combined = Sets.union(previouslyMigrated, columns);

						synchronizeForwards(table, Sets.newHashSet(combined));
						copyData(table, previouslyMigrated, columns);
						this.migratedColumns.putAll(table, columns);
						break;
				}
			}
			catch (SQLException e) {
				throw new MigrationException(e);
			}
		}

		private void createGhostTables() throws MigrationException {
			try (Connection connection = backend.connect()) {
				TableCreator creator = new TableCreator();
				creator.create(connection, plan.getGhostTables());
			}
			catch (SQLException e) {
				throw new MigrationException(e);
			}
		}

		private void createIndexes() throws MigrationException {
			try (Connection connection = backend.connect()) {
				TableCreator creator = new TableCreator();
				creator.createIndexes(connection, plan.getGhostTables());
			}
			catch (SQLException e) {
				throw new MigrationException(e);
			}
		}

		private void synchronizeForwards(Table targetTable, Set<String> targetColumns) throws SQLException {
			log.info("Creating forward sync function for table: {}...", targetTable.getName());
			try (Connection connection = backend.connect()) {
				Catalog catalog = state.getCatalog();
				Multimap<TableRef, TableRef> tableMapping = state.getRefLog().getTableMapping(from, to);
				for (Entry<TableRef, TableRef> entry : tableMapping.entries()) {
					if (entry.getValue().getTableId().equals(targetTable.getName())) {
						TableRef source = entry.getKey();
						TableRef target = entry.getValue();
						ensureSyncFunctionExists(connection, refLog, source, target, catalog, targetColumns);
					}
				}
			}
		}

		private void copyData(Table targetTable, Set<String> migratedColumns, Set<String> columnsToMigrate)
				throws SQLException, InterruptedException {

			Catalog catalog = state.getCatalog();
			Multimap<TableRef, TableRef> tableMapping = state.getRefLog().getTableMapping(from, to);
			for (Entry<TableRef, TableRef> entry : tableMapping.entries()) {
				if (entry.getValue().getTableId().equals(targetTable.getName())) {
					Table source = catalog.getTable(entry.getKey().getTableId());
					Table target = catalog.getTable(entry.getValue().getTableId());
					TableDataMigrator tableDataMigrator = new TableDataMigrator(backend, refLog);
					tableDataMigrator.migrateData(nullRecords, source, target, migratedColumns, columnsToMigrate);
				}
			}
		}

		private void synchronizeBackwards() throws MigrationException {
			log.info("Creating backwards sync functions...");
			try (Connection connection = backend.connect()) {
				Catalog catalog = state.getCatalog();
				Multimap<TableRef, TableRef> tableMapping = state.getRefLog().getTableMapping(from, to);
				for (Entry<TableRef, TableRef> entry : tableMapping.entries()) {
					TableRef source = entry.getKey();
					TableRef target = entry.getValue();
					Table targetTable = catalog.getTable(target.getTableId());

					Set<String> columns = targetTable.getColumns().stream()
							.map(Column::getName)
							.collect(Collectors.toSet());

					log.info("Creating backward sync function for table: {}...", target.getName());
					ensureSyncFunctionExists(connection, refLog, source, target, catalog, columns);
				}
			}
			catch (SQLException e) {
				throw new MigrationException(e);
			}
		}

		void ensureSyncFunctionExists(Connection connection, RefLog refLog, TableRef source,
				TableRef target, Catalog catalog, Set<String> columns) throws SQLException {

			String sourceTableId = source.getTableId();
			String targetTableId = target.getTableId();

			MigrationFunctions functions = state.getFunctions();
			SyncFunction syncFunction = syncFunctions.get(sourceTableId, targetTableId);
			if (syncFunction == null) {
				syncFunction = new SyncFunction(refLog, source, target, catalog, nullRecords);
				syncFunction.setColumnsToMigrate(columns);
				syncFunctions.put(sourceTableId, targetTableId, syncFunction);

				log.info("Creating sync function: {} for table: {}", syncFunction.getFunctionName(), sourceTableId);
				execute(connection, syncFunction.createFunctionStatement());
				functions.putFunction(sourceTableId, targetTableId, syncFunction.getFunctionName());

				log.info("Creating trigger: {} for table: {}", syncFunction.getTriggerName(), sourceTableId);
				execute(connection, syncFunction.createTriggerStatement());
				functions.putTrigger(sourceTableId, targetTableId, syncFunction.getTriggerName());
			}
			else {
				syncFunction.setColumnsToMigrate(columns);

				log.info("Updating sync function: {} for table: {}", syncFunction.getFunctionName(), sourceTableId);
				execute(connection, syncFunction.createFunctionStatement());
				functions.putFunction(sourceTableId, targetTableId, syncFunction.getFunctionName());
			}
		}

		private void execute(Connection connection, QueryBuilder queryBuilder) throws SQLException {
			String query = queryBuilder.toString();
			try (Statement statement = connection.createStatement()) {
				log.debug("Executing: " + query);
				statement.execute(query);
			}
		}
	}
}
