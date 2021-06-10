package io.quantumdb.core.planner;

import static com.google.common.base.Preconditions.checkArgument;
import static io.quantumdb.core.planner.QueryUtils.quoted;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.DatabaseMigrator;
import io.quantumdb.core.backends.planner.Operation;
import io.quantumdb.core.backends.planner.Plan;
import io.quantumdb.core.backends.planner.PlanValidator;
import io.quantumdb.core.backends.planner.Step;
import io.quantumdb.core.backends.postgresql.migrator.ViewCreator;
import io.quantumdb.core.migration.Migrator.Stage;
import io.quantumdb.core.migration.VersionTraverser.Direction;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Sequence;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DataOperation;
import io.quantumdb.core.schema.operations.Operation.Type;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.SyncRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import io.quantumdb.query.rewriter.PostgresqlQueryRewriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class PostgresqlMigrator implements DatabaseMigrator {

	private final PostgresqlBackend backend;
	private final Config config;

	PostgresqlMigrator(PostgresqlBackend backend, Config config) {
		this.backend = backend;
		this.config = config;
	}

	@Override
	public void applySchemaChanges(State state, Version from, Version to) throws MigrationException {
		RefLog refLog = state.getRefLog();
		Set<Version> preMigration = refLog.getVersions();
		Plan plan = new PostgresqlMigrationPlanner().createPlan(state, from, to);

		PlanValidator.validate(plan);
		Set<Version> postMigration = refLog.getVersions();
		Set<Version> intermediateVersions = Sets.newHashSet(Sets.difference(postMigration, preMigration));
		intermediateVersions.remove(to);

		new InternalPlanner(backend, config, plan, state, from, to, intermediateVersions).migrate();
	}

	@Override
	public void applyDataChanges(State state, Stage stage) throws MigrationException {
		List<Version> versions = stage.getVersions();
		Map<Version, DataOperation> operations = Maps.newLinkedHashMap();
		for (Version version : versions) {
			io.quantumdb.core.schema.operations.Operation operation = version.getOperation();
			if (!(operation instanceof DataOperation)) {
				throw new IllegalArgumentException("This stage contains non-data steps!");
			}

			DataOperation dataOperation = (DataOperation) operation;
			operations.put(version, dataOperation);
		}

		RefLog refLog = state.getRefLog();

		PostgresqlQueryRewriter rewriter = new PostgresqlQueryRewriter();
		Map<String, String> mapping = refLog.getTableRefs(stage.getParent()).stream()
				.collect(Collectors.toMap(TableRef::getName, TableRef::getRefId));
		rewriter.setTableMapping(mapping);

		try (Connection connection = backend.connect()) {
			connection.setAutoCommit(false);
			for (Entry<Version, DataOperation> entry : operations.entrySet()) {
				try {
					DataOperation dataOperation = entry.getValue();
					String query = dataOperation.getQuery();
					String rewrittenQuery = rewriter.rewrite(query);
					QueryUtils.execute(connection, config, rewrittenQuery);
					refLog.fork(entry.getKey());
				}
				catch (SQLException e) {
					connection.rollback();
					throw e;
				}
			}
			connection.commit();
			backend.persistState(state);
		}
		catch (SQLException e) {
			throw new MigrationException("Exception happened while performing data changes.", e);
		}
	}

	@Override
	public void drop(State state, Version version, Stage stage) throws MigrationException {
		// Check that the version's operation is of type DDL, or has no operation (root version).
		checkArgument(version.getOperation() == null || version.getOperation().getType() == Type.DDL);

		RefLog refLog = state.getRefLog();
		Catalog catalog = state.getCatalog();

		Set<Version> activeVersions = refLog.getVersions().stream()
				.filter(version1 -> !version1.equals(version))
				.collect(Collectors.toSet());

		// Add latest version of intermediate steps
		if (stage != null) {
			activeVersions.add(stage.getLast());
		}

		// Drop tables that are not part of any active versions or last intermediate versions
		List<TableRef> tablesToDrop = refLog.getTableRefs().stream()
				.filter(tableRef -> tableRef.getVersions().contains(version))
				// Get only tableRefs that are also not part of any active versions
				.filter(tableRef -> tableRef.getVersions().stream().noneMatch(activeVersions::contains))
				.filter(tableRef -> refLog.getTableRefs().stream()
						// If a TableRef exists with the same RefId
						.filter(tableRef1 -> tableRef.getRefId().equals(tableRef1.getRefId()))
						// But is not the same TableRef object (because of the renameTable migration)
						.filter(tableRef1 -> !tableRef.equals(tableRef1))
						// Do not drop the table if that TableRef object is still part of an active version
						.noneMatch(tableRef1 -> tableRef1.getVersions().stream().anyMatch(activeVersions::contains)))
				// Because the table may have already been dropped, only select the tables that are present in the catalog
				.filter(tableRef -> catalog.getTables().stream().anyMatch(table -> tableRef.getRefId().equals(table.getName())))
				.collect(Collectors.toList());

		Map<SyncRef, SyncFunction> newSyncFunctions = Maps.newLinkedHashMap();

		log.info("Determined the following tables will be dropped: {}", tablesToDrop);
		for (TableRef tableRef : tablesToDrop) {
			Set<SyncRef> inbounds = tableRef.getInboundSyncs();
			Set<SyncRef> outbounds = tableRef.getOutboundSyncs();

			for (SyncRef inbound : inbounds) {
				TableRef source = inbound.getSource();
				Direction direction = inbound.getDirection();

				Map<ColumnRef, ColumnRef> inboundMapping = inbound.getColumnMapping();
				for (SyncRef outbound : outbounds) {
					if (outbound.getDirection() != direction) {
						continue;
					}

					TableRef target = outbound.getTarget();
					Map<ColumnRef, ColumnRef> outboundMapping = outbound.getColumnMapping();

					// TODO: Somehow persist default values ?
					Map<ColumnRef, ColumnRef> newMapping = inboundMapping.entrySet().stream()
							.filter(entry -> {
								ColumnRef intermediate = entry.getValue();
								return outboundMapping.containsKey(intermediate);
							})
							.collect(Collectors.toMap(Entry::getKey, entry -> outboundMapping.get(entry.getValue())));

					Set<String> columnsToMigrate = newMapping.values().stream()
							.map(ColumnRef::getName)
							.collect(Collectors.toSet());

					NullRecords nullRecords = new NullRecords(config);
					SyncFunction sync = new SyncFunction(refLog, source, target, newMapping, catalog, nullRecords);
					sync.setColumnsToMigrate(columnsToMigrate);

					SyncRef syncRef = refLog.addSync(sync.getTriggerName(), sync.getFunctionName(), newMapping);
					newSyncFunctions.put(syncRef, sync);
				}
			}
		}

		try (Connection connection = backend.connect()) {
			connection.setAutoCommit(false);

			dropSynchronizers(connection, state.getRefLog(), tablesToDrop);
			for (SyncFunction syncFunction : newSyncFunctions.values()) {
				QueryUtils.execute(connection, config, syncFunction.createFunctionStatement().toString());
				QueryUtils.execute(connection, config, syncFunction.createTriggerStatement().toString());
			}
			dropTables(connection, refLog, catalog, tablesToDrop);
			refLog.setVersionState(version, false);
			backend.persistState(state);
			connection.commit();
		}
		catch (SQLException e) {
			throw new MigrationException(e);
		}
	}

	private void dropSynchronizers(Connection connection, RefLog refLog, List<TableRef> tablesToDrop)
			throws SQLException {

		connection.setAutoCommit(false);

		for (TableRef table : tablesToDrop) {
			String refId = table.getRefId();
			TableRef tableRef = refLog.getTableRefById(refId);
			Set<SyncRef> tableSyncs = Sets.newHashSet();
			tableSyncs.addAll(tableRef.getInboundSyncs());
			tableSyncs.addAll(tableRef.getOutboundSyncs());

			for (SyncRef tableSync : tableSyncs) {
				dropSynchronizer(connection, tableSync);
			}
		}

		connection.commit();
	}

	private void dropSynchronizer(Connection connection, SyncRef sync) throws SQLException {
		String triggerName = sync.getName();
		String functionName = sync.getFunctionName();
		String sourceRefId = sync.getSource().getRefId();
		String targetRefId = sync.getTarget().getRefId();

		QueryUtils.execute(connection, config, "DROP TRIGGER " + quoted(triggerName) + " ON " + quoted(sourceRefId) + ";");
		QueryUtils.execute(connection, config, "DROP FUNCTION " + quoted(functionName) + "();");
		sync.drop();
		log.info("Dropped synchronizer: {}/{} for: {} -> {}", triggerName, functionName, sourceRefId, targetRefId);
	}

	private void dropTables(Connection connection, RefLog refLog, Catalog catalog, List<TableRef> tablesToDrop)
			throws SQLException {

		connection.setAutoCommit(false);

		Set<String> refIdsToDrop = tablesToDrop.stream()
				.map(TableRef::getRefId)
				.collect(Collectors.toSet());

		for (TableRef tableRef : tablesToDrop) {
			String refId = tableRef.getRefId();
			Table table = catalog.getTable(refId);

			Set<Sequence> usedSequences = table.getColumns().stream()
					.map(Column::getSequence)
					.filter(Objects::nonNull)
					.collect(Collectors.toSet());

			Set<Table> tablesNotToBeDeleted = catalog.getTables().stream()
					.filter(otherTable -> !refIdsToDrop.contains(otherTable.getName()))
					.collect(Collectors.toSet());

			for (Table otherTable : tablesNotToBeDeleted) {
				boolean reassigned = false;
				for (Column column : otherTable.getColumns()) {
					Sequence sequence = column.getSequence();
					if (sequence != null && usedSequences.contains(sequence)) {
						String sequenceName = sequence.getName();
						String target = quoted(otherTable.getName()) + "." + quoted(column.getName());
						log.info("Reassigning sequence: {} to: {}", sequenceName, target);
						QueryUtils.execute(connection, config, "ALTER SEQUENCE " + quoted(sequenceName) + " OWNED BY " + target + ";");
						usedSequences.remove(sequence);
						reassigned = true;
						break;
					}
				}

				if (reassigned) {
					break;
				}
			}

			QueryUtils.execute(connection, config, "DROP TABLE " + quoted(refId) + " CASCADE;");
		}

		connection.commit();

		// Drop tables from the reflog.
		tablesToDrop.forEach(refLog::dropTable);

		// Drop all FKs connected to any table that is to be dropped.
		tablesToDrop.forEach(tableRef -> {
			Table table = catalog.getTable(tableRef.getRefId());
			// Defensive copy to avoid concurrent modification
			List<ForeignKey> foreignKeys = Lists.newArrayList(table.getForeignKeys());
			foreignKeys.forEach(ForeignKey::drop);
		});

		// Drop tables from the catalog.
		tablesToDrop.forEach(tableRef -> catalog.removeTable(tableRef.getRefId()));
	}

	static class InternalPlanner {

		private final Plan plan;
		private final Set<Version> intermediateVersions;
		private final RefLog refLog;
		private final State state;
		private final NullRecords nullRecords;
		private final Multimap<Table, String> migratedColumns;
		private final PostgresqlBackend backend;
		private final Config config;
		private final Version from;
		private final Version to;

		private final com.google.common.collect.Table<String, String, SyncFunction> syncFunctions;


		public InternalPlanner(PostgresqlBackend backend, Config config, Plan plan, State state,
				Version from, Version to, Set<Version> intermediateVersions) {

			this.backend = backend;
			this.config = config;
			this.plan = plan;
			this.intermediateVersions = intermediateVersions;
			this.refLog = plan.getRefLog();
			this.state = state;
			this.nullRecords = new NullRecords(config);
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

			refLog.setVersionState(to, true);

			createViews(to);

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

		private void createViews(Version version) throws MigrationException {
			try (Connection connection = backend.connect()) {
				ViewCreator creator = new ViewCreator();
				creator.create(connection, plan.getViews(), refLog, version);
			}
			catch (SQLException e) {
				throw new MigrationException(e);
			}
		}

		private void createGhostTables() throws MigrationException {
			try (Connection connection = backend.connect()) {
				TableCreator creator = new TableCreator(config);
				creator.createTables(connection, plan.getGhostTables());
				creator.createForeignKeys(connection, plan.getGhostTables());
			}
			catch (SQLException e) {
				throw new MigrationException(e);
			}
		}

		private void createIndexes() throws MigrationException {
			try (Connection connection = backend.connect()) {
				TableCreator creator = new TableCreator(config);
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
					if (entry.getValue().getRefId().equals(targetTable.getName())) {
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
				if (entry.getValue().getRefId().equals(targetTable.getName())) {
					Table source = catalog.getTable(entry.getKey().getRefId());
					Table target = catalog.getTable(entry.getValue().getRefId());
					TableDataMigrator tableDataMigrator = new TableDataMigrator(backend, config, refLog);
					tableDataMigrator.migrateData(nullRecords, source, target, from, to, migratedColumns, columnsToMigrate);
				}
			}
		}

		private void synchronizeBackwards() throws MigrationException {
			log.info("Creating backwards sync functions...");
			try (Connection connection = backend.connect()) {
				Catalog catalog = state.getCatalog();
				Multimap<TableRef, TableRef> tableMapping = state.getRefLog().getTableMapping(from, to);
				for (Entry<TableRef, TableRef> entry : tableMapping.entries()) {
					TableRef target = entry.getKey();
					TableRef source = entry.getValue();
					Table targetTable = catalog.getTable(target.getRefId());

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

			String sourceRefId = source.getRefId();
			String targetRefId = target.getRefId();

			SyncFunction syncFunction = syncFunctions.get(sourceRefId, targetRefId);
			if (syncFunction == null) {
				Map<ColumnRef, ColumnRef> mapping = refLog.getColumnMapping(source, target);
				syncFunction = new SyncFunction(refLog, source, target, mapping, catalog, nullRecords);
				syncFunction.setColumnsToMigrate(columns);
				syncFunctions.put(sourceRefId, targetRefId, syncFunction);

				log.info("Creating sync function: {} for table: {}", syncFunction.getFunctionName(), sourceRefId);
				QueryUtils.execute(connection, config, syncFunction.createFunctionStatement().toString());

				log.info("Creating trigger: {} for table: {}", syncFunction.getTriggerName(), sourceRefId);
				QueryUtils.execute(connection, config, syncFunction.createTriggerStatement().toString());

				Map<ColumnRef, ColumnRef> columnMapping = refLog.getColumnMapping(source, target);
				refLog.addSync(syncFunction.getTriggerName(), syncFunction.getFunctionName(), columnMapping);
			}
			else {
				syncFunction.setColumnsToMigrate(columns);

				log.info("Updating sync function: {} for table: {}", syncFunction.getFunctionName(), sourceRefId);
				QueryUtils.execute(connection, config, syncFunction.createFunctionStatement().toString());

				TableRef sourceTable = refLog.getTableRefById(sourceRefId);
				sourceTable.getOutboundSyncs().stream()
						.filter(ref -> ref.getTarget().equals(target))
						.forEach(ref -> refLog.getColumnMapping(source, target).forEach((from, to) -> {
							boolean exists = ref.getColumnMapping().entrySet().stream()
									.anyMatch(entry -> entry.getKey().equals(from) && entry.getValue().equals(to));
							if (!exists) {
								ref.addColumnMapping(from, to);
							}
						}));

				TableRef targetTable = refLog.getTableRefById(targetRefId);
				targetTable.getInboundSyncs().stream()
						.filter(ref -> ref.getSource().equals(source))
						.forEach(ref -> refLog.getColumnMapping(target, source).forEach((from, to) -> {
							boolean exists = ref.getColumnMapping().entrySet().stream()
									.anyMatch(entry -> entry.getKey().equals(from) && entry.getValue().equals(to));
							if (!exists) {
								ref.addColumnMapping(from, to);
							}
						}));
			}
		}

	}

}
