package io.quantumdb.core.backends.integration.types;

import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.BINARY_ID;
import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.BOOLEAN_ID;
import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.CHARACTER_ID;
import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.DATE_ID;
import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.NUMERIC_BIG_ID;
import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.NUMERIC_ID;
import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.NUMERIC_SMALL_ID;
import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.OID_ID;
import static io.quantumdb.core.backends.integration.types.PostgresqlTypesScenario.UUID_ID;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.Column.Hint.PRIMARY_KEY;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bool;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bytea;
import static io.quantumdb.core.schema.definitions.PostgresTypes.chars;
import static io.quantumdb.core.schema.definitions.PostgresTypes.date;
import static io.quantumdb.core.schema.definitions.PostgresTypes.doubles;
import static io.quantumdb.core.schema.definitions.PostgresTypes.floats;
import static io.quantumdb.core.schema.definitions.PostgresTypes.integer;
import static io.quantumdb.core.schema.definitions.PostgresTypes.numeric;
import static io.quantumdb.core.schema.definitions.PostgresTypes.oid;
import static io.quantumdb.core.schema.definitions.PostgresTypes.smallint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.text;
import static io.quantumdb.core.schema.definitions.PostgresTypes.timestamp;
import static io.quantumdb.core.schema.definitions.PostgresTypes.uuid;
import static io.quantumdb.core.schema.definitions.PostgresTypes.varchar;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.utils.BatchInserter;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class CopyTypesTables {

	@ClassRule
	public static PostgresqlTypesScenario setup = new PostgresqlTypesScenario();

	private static State state;
	private static Version origin;
	private static Version target;

	@BeforeClass
	public static void performEvolution() throws SQLException, MigrationException {
		setup.insertTestData();

		origin = setup.getChangelog().getLastAdded();

		setup.getChangelog().addChangeSet("Copy Types Tables", "Nick Richter",
				SchemaOperations.copyTable("numeric_small", "numeric_small_backup"),
				SchemaOperations.copyTable("numeric", "numeric_backup"),
				SchemaOperations.copyTable("numeric_big", "numeric_big_backup"),
				SchemaOperations.copyTable("character", "character_backup"),
				SchemaOperations.copyTable("binary", "binary_backup"),
				SchemaOperations.copyTable("date", "date_backup"),
				SchemaOperations.copyTable("bool", "bool_backup"),
				SchemaOperations.copyTable("uuid", "uuid_backup"),
				SchemaOperations.copyTable("oid", "oid_backup"));

		target = setup.getChangelog().getLastAdded();
		setup.getBackend().persistState(setup.getState());

		setup.getMigrator().migrate(origin.getId(), target.getId());

		state = setup.getBackend().loadState();
	}

	@Test
	public void verifyTableStructure() {
		RefLog refLog = state.getRefLog();

		// Original tables.

		Table numeric_small = new Table(refLog.getTableRef(origin, "numeric_small").getRefId())
				// Serial will automatically create a sequence (AUTO_INCREMENT) and NOT_NULL constraint
				.addColumn(new Column("id", smallint(), PRIMARY_KEY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("smallint", smallint(), NOT_NULL))
				.addColumn(new Column("smallnumeric_integer", numeric(5, 0), NOT_NULL))
				.addColumn(new Column("smallnumeric_decimal", numeric(5, 2), NOT_NULL))
				.addColumn(new Column("smallnumeric", numeric(5,0), NOT_NULL));

		Table numeric = new Table(refLog.getTableRef(origin, "numeric").getRefId())
				.addColumn(new Column("id", integer(), PRIMARY_KEY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("int", integer(), NOT_NULL))
				.addColumn(new Column("float", floats(), NOT_NULL))
				.addColumn(new Column("double", doubles(), NOT_NULL))
				.addColumn(new Column("numeric_integer", numeric(10, 0), NOT_NULL))
				.addColumn(new Column("numeric_decimal", numeric(10, 5), NOT_NULL))
				.addColumn(new Column("numeric", numeric(10,0), NOT_NULL));

		Table numeric_big = new Table(refLog.getTableRef(origin, "numeric_big").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("bigint", bigint(), NOT_NULL))
				.addColumn(new Column("bignumeric_integer", numeric(1000, 0), NOT_NULL))
				.addColumn(new Column("bignumeric_decimal", numeric(1000, 100), NOT_NULL))
				.addColumn(new Column("bignumeric", numeric(), NOT_NULL));

		Table character = new Table(refLog.getTableRef(origin, "character").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("varchar_unlimited", varchar(), NOT_NULL))
				.addColumn(new Column("varchar_10", varchar(10), NOT_NULL))
				.addColumn(new Column("varchar_100", varchar(100), NOT_NULL))
				.addColumn(new Column("char", chars(1), NOT_NULL))
				.addColumn(new Column("char_100", chars(100), NOT_NULL))
				.addColumn(new Column("text", text(), NOT_NULL));

		Table binary = new Table(refLog.getTableRef(origin, "binary").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("bytea", bytea(), NOT_NULL));

		Table date = new Table(refLog.getTableRef(origin, "date").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("timestamp_0", timestamp(false, 0), NOT_NULL))
				.addColumn(new Column("timestamp_6", timestamp(false, 6), NOT_NULL))
				.addColumn(new Column("timestamptz_0", timestamp(true, 0), NOT_NULL))
				.addColumn(new Column("timestamptz_6", timestamp(false, 6), NOT_NULL));

		Table bool = new Table(refLog.getTableRef(origin, "bool").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("bool", bool(), NOT_NULL));

		Table uuid = new Table(refLog.getTableRef(origin, "uuid").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("uuid", uuid(), NOT_NULL));

		Table oid = new Table(refLog.getTableRef(origin, "oid").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("oid", oid(), NOT_NULL));

		// New tables.

		Table new_numeric_small = new Table(refLog.getTableRef(target, "numeric_small_backup").getRefId())
				// Serial will automatically create a sequence (AUTO_INCREMENT) and NOT_NULL constraint
				.addColumn(new Column("id", smallint(), PRIMARY_KEY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("smallint", smallint(), NOT_NULL))
				.addColumn(new Column("smallnumeric_integer", numeric(5, 0), NOT_NULL))
				.addColumn(new Column("smallnumeric_decimal", numeric(5, 2), NOT_NULL))
				.addColumn(new Column("smallnumeric", numeric(5,0), NOT_NULL));

		Table new_numeric = new Table(refLog.getTableRef(target, "numeric_backup").getRefId())
				.addColumn(new Column("id", integer(), PRIMARY_KEY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("int", integer(), NOT_NULL))
				.addColumn(new Column("float", floats(), NOT_NULL))
				.addColumn(new Column("double", doubles(), NOT_NULL))
				.addColumn(new Column("numeric_integer", numeric(10, 0), NOT_NULL))
				.addColumn(new Column("numeric_decimal", numeric(10, 5), NOT_NULL))
				.addColumn(new Column("numeric", numeric(10,0), NOT_NULL));

		Table new_numeric_big = new Table(refLog.getTableRef(target, "numeric_big_backup").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("bigint", bigint(), NOT_NULL))
				.addColumn(new Column("bignumeric_integer", numeric(1000, 0), NOT_NULL))
				.addColumn(new Column("bignumeric_decimal", numeric(1000, 100), NOT_NULL))
				.addColumn(new Column("bignumeric", numeric(), NOT_NULL));

		Table new_character = new Table(refLog.getTableRef(target, "character_backup").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("varchar_unlimited", varchar(), NOT_NULL))
				.addColumn(new Column("varchar_10", varchar(10), NOT_NULL))
				.addColumn(new Column("varchar_100", varchar(100), NOT_NULL))
				.addColumn(new Column("char", chars(1), NOT_NULL))
				.addColumn(new Column("char_100", chars(100), NOT_NULL))
				.addColumn(new Column("text", text(), NOT_NULL));

		Table new_binary = new Table(refLog.getTableRef(target, "binary_backup").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("bytea", bytea(), NOT_NULL));

		Table new_date = new Table(refLog.getTableRef(target, "date_backup").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("timestamp_0", timestamp(false, 0), NOT_NULL))
				.addColumn(new Column("timestamp_6", timestamp(false, 6), NOT_NULL))
				.addColumn(new Column("timestamptz_0", timestamp(true, 0), NOT_NULL))
				.addColumn(new Column("timestamptz_6", timestamp(false, 6), NOT_NULL));

		Table new_bool = new Table(refLog.getTableRef(target, "bool_backup").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("bool", bool(), NOT_NULL));

		Table new_uuid = new Table(refLog.getTableRef(target, "uuid_backup").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("uuid", uuid(), NOT_NULL));

		Table new_oid = new Table(refLog.getTableRef(target, "oid_backup").getRefId())
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("oid", oid(), NOT_NULL));

		List<Table> tables = Lists.newArrayList(numeric_small, numeric, numeric_big, character, binary, date, bool, uuid,
				oid, new_numeric_small, new_numeric, new_numeric_big, new_character, new_binary, new_date, new_bool, new_uuid,
				new_oid);

		Catalog expected = new Catalog(setup.getCatalog().getName());
		tables.forEach(expected::addTable);

		assertEquals(expected.getTables(), state.getCatalog().getTables());
	}

	@Test
	public void verifyTableMappings() {
		RefLog refLog = state.getRefLog();

		// Unchanged tables
		assertEquals(NUMERIC_SMALL_ID, refLog.getTableRef(target, "numeric_small").getRefId());
		assertEquals(NUMERIC_ID, refLog.getTableRef(target, "numeric").getRefId());
		assertEquals(NUMERIC_BIG_ID, refLog.getTableRef(target, "numeric_big").getRefId());
		assertEquals(CHARACTER_ID, refLog.getTableRef(target, "character").getRefId());
		assertEquals(BINARY_ID, refLog.getTableRef(target, "binary").getRefId());
		assertEquals(DATE_ID, refLog.getTableRef(target, "date").getRefId());
		assertEquals(BOOLEAN_ID, refLog.getTableRef(target, "bool").getRefId());
		assertEquals(UUID_ID, refLog.getTableRef(target, "uuid").getRefId());
		assertEquals(OID_ID, refLog.getTableRef(target, "oid").getRefId());

		// New tables
		assertNotNull(refLog.getTableRef(target, "numeric_small_backup").getRefId());
		assertNotNull(refLog.getTableRef(target, "numeric_backup").getRefId());
		assertNotNull(refLog.getTableRef(target, "numeric_big_backup").getRefId());
		assertNotNull(refLog.getTableRef(target, "character_backup").getRefId());
		assertNotNull(refLog.getTableRef(target, "binary_backup").getRefId());
		assertNotNull(refLog.getTableRef(target, "date_backup").getRefId());
		assertNotNull(refLog.getTableRef(target, "bool_backup").getRefId());
		assertNotNull(refLog.getTableRef(target, "uuid_backup").getRefId());
		assertNotNull(refLog.getTableRef(target, "oid_backup").getRefId());

	}

	@Test
	public void verifyCorrectValuesInCopiedTables() throws SQLException {
		insertTestDataInCopiedTables();

		Connection connection = setup.getConnection();
		RefLog refLog = state.getRefLog();

		try {
			Statement statement = connection.createStatement();

			ResultSet numeric_small_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "numeric_small_backup").getRefId());

			numeric_small_set.next();
			assertEquals(1, numeric_small_set.getInt("smallint"));
			assertEquals(new BigDecimal("1"), numeric_small_set.getBigDecimal("smallnumeric_integer"));
			assertEquals(new BigDecimal("123.45"), numeric_small_set.getBigDecimal("smallnumeric_decimal"));
			assertEquals(new BigDecimal("12345"), numeric_small_set.getBigDecimal("smallnumeric"));

			numeric_small_set.next();
			assertEquals(2, numeric_small_set.getInt("smallint"));
			assertEquals(new BigDecimal("2"), numeric_small_set.getBigDecimal("smallnumeric_integer"));
			assertEquals(new BigDecimal("123.46"), numeric_small_set.getBigDecimal("smallnumeric_decimal"));
			assertEquals(new BigDecimal("1235"), numeric_small_set.getBigDecimal("smallnumeric"));

			ResultSet numeric_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "numeric_backup").getRefId());

			numeric_set.next();
			assertEquals(1, numeric_set.getInt("int"));
			assertEquals(1.23456f, numeric_set.getFloat("float"), 0.00001f);
			assertEquals(1.234567890123456d, numeric_set.getDouble("double"), 0.000000000000001d);
			assertEquals(new BigDecimal("1000000000"), numeric_set.getBigDecimal("numeric_integer"));
			assertEquals(new BigDecimal("1234.56789"), numeric_set.getBigDecimal("numeric_decimal"));
			assertEquals(new BigDecimal("124"), numeric_set.getBigDecimal("numeric"));

			numeric_set.next();
			assertEquals(2, numeric_set.getInt("int"));
			assertEquals(123f, numeric_set.getFloat("float"), 0.00001f);
			assertEquals(123d, numeric_set.getDouble("double"), 0.000000000000001d);
			assertEquals(new BigDecimal("123"), numeric_set.getBigDecimal("numeric_integer"));
			assertEquals(new BigDecimal("123.00000"), numeric_set.getBigDecimal("numeric_decimal"));
			assertEquals(new BigDecimal("123"), numeric_set.getBigDecimal("numeric"));

			ResultSet numeric_big_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "numeric_big_backup").getRefId());

			numeric_big_set.next();
			assertEquals(9223372036854775807L, numeric_big_set.getLong("bigint"));
			assertEquals(new BigDecimal("1234567890123456789012345678901234567890"), numeric_big_set.getBigDecimal("bignumeric_integer"));
			assertEquals(new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789012345678901234567890000000000000000000000000000000000000000000000000000000000000"), numeric_big_set.getBigDecimal("bignumeric_decimal"));
			assertEquals(new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789012345678901234567890"), numeric_big_set.getBigDecimal("bignumeric"));

			ResultSet character_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "character_backup").getRefId());

			character_set.next();
			assertEquals("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut " +
					"labore et dolore magna aliqua. Sit amet cursus sit amet dictum sit amet. Cras fermentum odio " +
					"eu feugiat pretium nibh ipsum. Viverra nam libero justo laoreet sit amet cursus sit amet. Nun" +
					"c scelerisque viverra mauris in aliquam sem fringilla. Varius duis at consectetur lorem. Cons" +
					"ectetur adipiscing elit pellentesque habitant morbi tristique senectus et. Urna neque viverra" +
					" justo nec ultrices dui sapien eget. Vel facilisis volutpat est velit egestas dui. Facilisis " +
					"magna etiam tempor orci eu lobortis. Cursus mattis molestie a iaculis at erat pellentesque. I" +
					"n arcu cursus euismod quis viverra. Massa enim nec dui nunc mattis enim ut tellus elementum.", character_set.getString("varchar_unlimited"));
			assertEquals("max 10 cha", character_set.getString("varchar_10"));
			assertEquals("max 100 characters", character_set.getString("varchar_100"));
			assertEquals("t", character_set.getString("char"));
			assertEquals("space padded to 100                                                                                 ", character_set.getString("char_100"));
			assertEquals("Lorem Ipsum", character_set.getString("text"));

			character_set.next();
			assertEquals("Lorem Ipsum", character_set.getString("varchar_unlimited"));
			assertEquals("string_ten", character_set.getString("varchar_10"));
			assertEquals("String, max 100 characters", character_set.getString("varchar_100"));
			assertEquals("f", character_set.getString("char"));
			assertEquals("space padded to 100                                                                                 ", character_set.getString("char_100"));
			assertEquals("Lorem Ipsum", character_set.getString("text"));

			ResultSet binary_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "binary_backup").getRefId());

			binary_set.next();
			assertEquals("pdf_file", new String(binary_set.getBytes("bytea")));

			ResultSet date_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "date_backup").getRefId());

			date_set.next();
			assertEquals(1599948000000L, date_set.getDate("date").getTime());
			assertEquals(1600000000000L, date_set.getTimestamp("timestamp_0").getTime());
			assertEquals(0, date_set.getTimestamp("timestamp_0").getNanos());
			assertEquals(1600000000123L, date_set.getTimestamp("timestamp_6").getTime());
			assertEquals(123457000, date_set.getTimestamp("timestamp_6").getNanos());
			assertEquals(1600000000000L, date_set.getTimestamp("timestamptz_0").getTime());
			assertEquals(0, date_set.getTimestamp("timestamptz_0").getNanos());
			assertEquals(1600000000123L, date_set.getTimestamp("timestamptz_6").getTime());
			assertEquals(123457000, date_set.getTimestamp("timestamptz_6").getNanos());

			ResultSet boolean_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "bool_backup").getRefId());

			boolean_set.next();
			assertTrue(boolean_set.getBoolean("bool"));

			boolean_set.next();
			assertFalse(boolean_set.getBoolean("bool"));

			ResultSet uuid_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "uuid_backup").getRefId());

			uuid_set.next();
			assertEquals(UUID.fromString("4cc75deb-cd5f-49e8-b94c-c692103e370c"), (UUID) uuid_set.getObject("uuid"));

			ResultSet oid_set = statement.executeQuery("SELECT * FROM " + refLog.getTableRef(target, "oid_backup").getRefId());

			oid_set.next();
			assertEquals(1234, oid_set.getInt("oid"));

		}
		catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		
	}

	public void insertTestDataInCopiedTables() throws SQLException {
		RefLog refLog = state.getRefLog();
		
		Connection connection = setup.getConnection();
		
		connection.setAutoCommit(false);
		connection.createStatement().execute("SET CONSTRAINTS ALL DEFERRED");

		BatchInserter.insertInto(connection, refLog.getTableRef(target, "numeric_small_backup").getRefId(), "smallint", "smallnumeric_integer", "smallnumeric_decimal", "smallnumeric")
				.values(1, new BigDecimal("1"), new BigDecimal("123.45"), new BigDecimal("12345"))
				.values(2, new BigDecimal("2"), new BigDecimal("123.456"), new BigDecimal("1234.5"))
				.insert();

		BatchInserter.insertInto(connection, refLog.getTableRef(target, "numeric_backup").getRefId(), "int", "float", "double", "numeric_integer", "numeric_decimal", "numeric")
				.values(1, 1.23456f, 1.234567890123456d, new BigDecimal("1000000000"), new BigDecimal("1234.567894"), new BigDecimal("123.9"))
				.values(2, 123f, 123d, new BigDecimal("123"), new BigDecimal("123"), new BigDecimal("123"))
				.insert();

		BatchInserter.insertInto(connection, refLog.getTableRef(target, "numeric_big_backup").getRefId(), "bigint", "bignumeric_integer", "bignumeric_decimal", "bignumeric")
				.values(9223372036854775807L, new BigDecimal("1234567890123456789012345678901234567890"),
						new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789012345678901234567890"),
						new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789012345678901234567890"))
				.insert();

		BatchInserter.insertInto(connection, refLog.getTableRef(target, "character_backup").getRefId(), "varchar_unlimited", "varchar_10", "varchar_100", "char", "char_100", "text")
				.values("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut " +
								"labore et dolore magna aliqua. Sit amet cursus sit amet dictum sit amet. Cras fermentum odio " +
								"eu feugiat pretium nibh ipsum. Viverra nam libero justo laoreet sit amet cursus sit amet. Nun" +
								"c scelerisque viverra mauris in aliquam sem fringilla. Varius duis at consectetur lorem. Cons" +
								"ectetur adipiscing elit pellentesque habitant morbi tristique senectus et. Urna neque viverra" +
								" justo nec ultrices dui sapien eget. Vel facilisis volutpat est velit egestas dui. Facilisis " +
								"magna etiam tempor orci eu lobortis. Cursus mattis molestie a iaculis at erat pellentesque. I" +
								"n arcu cursus euismod quis viverra. Massa enim nec dui nunc mattis enim ut tellus elementum.",
						"max 10 cha", "max 100 characters", "t", "space padded to 100", "Lorem Ipsum")
				.values("Lorem Ipsum", "string_ten", "String, max 100 characters",
						"f", "space padded to 100", "Lorem Ipsum")
				.insert();

		BatchInserter.insertInto(connection, refLog.getTableRef(target, "binary_backup").getRefId(), "bytea")
				.values("pdf_file".getBytes())
				.insert();

		Timestamp timestamp = new Timestamp(1600000000123L);
		timestamp.setNanos(123456789);
		BatchInserter.insertInto(connection, refLog.getTableRef(target, "date_backup").getRefId(), "date", "timestamp_0", "timestamp_6", "timestamptz_0", "timestamptz_6")
				.values(new Date(1600000000123L), timestamp,
						timestamp, timestamp,
						timestamp)
				.insert();

		BatchInserter.insertInto(connection, refLog.getTableRef(target, "bool_backup").getRefId(), "bool")
				.values(true)
				.values(false)
				.insert();

		BatchInserter.insertInto(connection, refLog.getTableRef(target, "uuid_backup").getRefId(), "uuid")
				.values(UUID.fromString("4cc75deb-cd5f-49e8-b94c-c692103e370c"))
				.insert();

		BatchInserter.insertInto(connection, refLog.getTableRef(target, "oid_backup").getRefId(), "oid")
				.values(1234)
				.insert();

		connection.setAutoCommit(true);
	}

}
