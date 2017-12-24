package io.quantumdb.core.versioning;

import static io.quantumdb.core.schema.definitions.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bool;
import static io.quantumdb.core.schema.definitions.PostgresTypes.text;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import com.google.common.collect.ImmutableMap;
import io.quantumdb.core.backends.PostgresqlDatabase;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Sequence;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class BackendTest {

	@Rule
	public final PostgresqlDatabase database = new PostgresqlDatabase();

	@Before
	public void setUp() throws SQLException {
		QuantumTables.prepare(database.createConnection());
	}

	@After
	public void tearDown() throws SQLException {
		QuantumTables.dropEverything(database.createConnection());
	}

	@Test
	public void testPersistingAndLoadingSimpleTestCase() throws SQLException {
		Sequence sequence = new Sequence("source_id_pk");
		Catalog catalog = new Catalog("public")
				.addSequence(sequence)
				.addTable(new Table("table_1")
						.addColumn(new Column("id", bigint(), sequence, AUTO_INCREMENT, IDENTITY))
						.addColumn(new Column("name", text(), NOT_NULL)))
				.addTable(new Table("table_2")
						.addColumn(new Column("id", bigint(), sequence, AUTO_INCREMENT, IDENTITY))
						.addColumn(new Column("name", text(), NOT_NULL))
						.addColumn(new Column("admin", bool(), "false", NOT_NULL)));

		Changelog changelog = new Changelog(RandomHasher.generateHash(), new ChangeSet("System", "Initial import"))
				.addChangeSet("Michael de Jong", addColumn("table", "admin", bool(), "false", NOT_NULL));

		RefLog refLog = new RefLog();
		TableRef table1 = refLog.addTable("table", "table_1", changelog.getRoot(),
				new ColumnRef("id"),
				new ColumnRef("name"));

		TableRef table2 = refLog.addTable("table", "table_2", changelog.getLastAdded(),
				new ColumnRef("id", table1.getColumn("id")),
				new ColumnRef("name", table1.getColumn("name")),
				new ColumnRef("admin"));

		refLog.addSync("trigger_1", "sync_1", ImmutableMap.<ColumnRef, ColumnRef>builder()
				.put(table1.getColumn("id"), table2.getColumn("id"))
				.put(table1.getColumn("name"), table2.getColumn("name"))
				.build());

		refLog.addSync("trigger_2", "sync_2", ImmutableMap.<ColumnRef, ColumnRef>builder()
				.put(table2.getColumn("id"), table1.getColumn("id"))
				.put(table2.getColumn("name"), table1.getColumn("name"))
				.build());

		Backend backend = new Backend();
		State expectedState = new State(catalog, refLog, changelog);
		backend.persist(database.createConnection(), expectedState);

		State actualState = backend.load(database.getConnection(), catalog);
		assertEquals(expectedState, actualState);
	}

}
