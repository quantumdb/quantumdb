package io.quantumdb.core.planner;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.Column.Hint.PRIMARY_KEY;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bool;
import static io.quantumdb.core.schema.definitions.PostgresTypes.text;
import static io.quantumdb.core.schema.definitions.PostgresTypes.timestamp;
import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.addForeignKey;
import static io.quantumdb.core.schema.operations.SchemaOperations.createTable;
import static org.junit.Assert.assertEquals;

import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.PostgresqlDatabase;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.SneakyThrows;
import org.junit.Rule;
import org.junit.Test;

public class PostgresqlMigratorTest {

	@Rule
	public final PostgresqlDatabase database = new PostgresqlDatabase();

	@Test
	@SneakyThrows
	public void testSimpleMigrationFromEmptyDatabase() {
		Config config = database.getConfig();
		PostgresqlBackend backend = new PostgresqlBackend(config);
		PostgresqlMigrator migrator = new PostgresqlMigrator(backend, config);
		State state = backend.loadState();

		Changelog changelog = state.getChangelog();
		changelog.addChangeSet("step-1", "Michael de Jong",
				createTable("users")
						.with("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT)
						.with("email", text(), NOT_NULL));

		Version version = changelog.getLastAdded();
		migrator.applySchemaChanges(state, changelog.getRoot(), version);

		Catalog catalog = state.getCatalog();
		RefLog refLog = state.getRefLog();
		assertEquals(Sets.newHashSet(version), refLog.getVersions());
		assertEquals(Sets.newHashSet(refLog.getTableRef(version, "users").getRefId()),
				catalog.getTables().stream().map(Table::getName).collect(Collectors.toSet()));
	}

	@Test
	@SneakyThrows
	public void testMigrationWithMultipleSchemaOperationsFromEmptyDatabase() {
		Config config = database.getConfig();
		PostgresqlBackend backend = new PostgresqlBackend(config);
		PostgresqlMigrator migrator = new PostgresqlMigrator(backend, config);
		State state = backend.loadState();

		Changelog changelog = state.getChangelog();
		changelog.addChangeSet("step-1", "Michael de Jong",
				createTable("users")
						.with("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT)
						.with("email", text(), NOT_NULL),
				addColumn("users", "first_name", text()),
				addColumn("users", "last_name", text()));

		Version version = changelog.getLastAdded();
		migrator.applySchemaChanges(state, changelog.getRoot(), version);
		
		state = backend.loadState(); // TODO: Should not be needed, but the planner pollutes the in-memory object.

		Catalog catalog = state.getCatalog();
		RefLog refLog = state.getRefLog();
		assertEquals(Sets.newHashSet(version), refLog.getVersions());
		assertEquals(Sets.newHashSet(refLog.getTableRef(version, "users").getRefId()),
				catalog.getTables().stream().map(Table::getName).collect(Collectors.toSet()));
	}

	@Test
	@SneakyThrows
	public void testDroppingSingleActiveVersion() {
		Config config = database.getConfig();
		PostgresqlBackend backend = new PostgresqlBackend(config);
		PostgresqlMigrator migrator = new PostgresqlMigrator(backend, config);
		State state = backend.loadState();

		Changelog changelog = state.getChangelog();
		changelog.addChangeSet("step-1", "Michael de Jong",
				createTable("users")
						.with("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT)
						.with("email", text(), NOT_NULL));

		Version version = changelog.getLastAdded();
		migrator.applySchemaChanges(state, changelog.getRoot(), version);
		migrator.drop(state, version, null);

		Catalog catalog = state.getCatalog();
		assertEquals(Sets.newHashSet(), state.getRefLog().getVersions());
		assertEquals(Sets.newHashSet(), catalog.getTables());
	}

	@Test
	@SneakyThrows
	public void testDroppingSingleActiveVersionConstructedWithMultipleOperations() {
		Config config = database.getConfig();
		PostgresqlBackend backend = new PostgresqlBackend(config);
		PostgresqlMigrator migrator = new PostgresqlMigrator(backend, config);
		State state = backend.loadState();

		Changelog changelog = state.getChangelog();
		changelog.addChangeSet("step-1", "Michael de Jong",
				createTable("users")
						.with("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT)
						.with("email", text(), NOT_NULL),
				createTable("messages")
						.with("author_id", bigint(), PRIMARY_KEY)
						.with("timestamp", timestamp(true), "NOW()", NOT_NULL)
						.with("message", text(), NOT_NULL),
				addForeignKey("messages", "author_id")
						.referencing("users", "id"));

		Version version = changelog.getLastAdded();
		migrator.applySchemaChanges(state, changelog.getRoot(), version);
		migrator.drop(state, version, null);

		state = backend.loadState(); // TODO: Should not be needed, but the planner pollutes the in-memory object.

		Catalog catalog = state.getCatalog();
		assertEquals(Sets.newHashSet(), state.getRefLog().getVersions());
		assertEquals(Sets.newHashSet(), catalog.getTables());
	}

	@Test
	@SneakyThrows
	public void testMigrationOfSingleSimpleSchemaOperation() {
		Config config = database.getConfig();
		PostgresqlBackend backend = new PostgresqlBackend(config);
		PostgresqlMigrator migrator = new PostgresqlMigrator(backend, config);
		State state = backend.loadState();

		Changelog changelog = state.getChangelog();
		changelog.addChangeSet("step-1", "Michael de Jong",
				createTable("users")
						.with("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT)
						.with("email", text(), NOT_NULL));

		Version step1 = changelog.getLastAdded();
		migrator.applySchemaChanges(state, changelog.getRoot(), step1);

		changelog.addChangeSet("step-2", "Michael de Jong",
				addColumn("users", "admin", bool(), "false", NOT_NULL));

		Version step2 = changelog.getLastAdded();
		migrator.applySchemaChanges(state, step1, step2);

		migrator.drop(state, step1, null);

		Catalog catalog = state.getCatalog();
		RefLog refLog = state.getRefLog();
		assertEquals(Sets.newHashSet(step2), refLog.getVersions());
		assertEquals(Sets.newHashSet(refLog.getTableRef(step2, "users").getRefId()),
				catalog.getTables().stream().map(Table::getName).collect(Collectors.toSet()));
	}

}
