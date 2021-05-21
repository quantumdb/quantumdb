package io.quantumdb.core.backends.integration.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class CheckCorrectValues {

	@ClassRule
	public static PostgresqlTypesScenario setup = new PostgresqlTypesScenario();

	@BeforeClass
	public static void performEvolution() throws SQLException, MigrationException {
		setup.insertTestData();
	}

	@Test
	public void verifyCorrectValues() {
		Connection connection = setup.getConnection();

		try {
			Statement statement = connection.createStatement();

			ResultSet numeric_small_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.NUMERIC_SMALL_ID);

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

			ResultSet numeric_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.NUMERIC_ID);

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

			ResultSet numeric_big_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.NUMERIC_BIG_ID);

			numeric_big_set.next();
			assertEquals(9223372036854775807L, numeric_big_set.getLong("bigint"));
			assertEquals(new BigDecimal("1234567890123456789012345678901234567890"), numeric_big_set.getBigDecimal("bignumeric_integer"));
			assertEquals(new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789012345678901234567890000000000000000000000000000000000000000000000000000000000000"), numeric_big_set.getBigDecimal("bignumeric_decimal"));
			assertEquals(new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789012345678901234567890"), numeric_big_set.getBigDecimal("bignumeric"));

			ResultSet character_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.CHARACTER_ID);

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

			ResultSet binary_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.BINARY_ID);

			binary_set.next();
			assertEquals("pdf_file", new String(binary_set.getBytes("bytea")));

			ResultSet date_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.DATE_ID);

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

			ResultSet boolean_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.BOOLEAN_ID);

			boolean_set.next();
			assertTrue(boolean_set.getBoolean("bool"));

			boolean_set.next();
			assertFalse(boolean_set.getBoolean("bool"));

			ResultSet uuid_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.UUID_ID);

			uuid_set.next();
			assertEquals(UUID.fromString("4cc75deb-cd5f-49e8-b94c-c692103e370c"), (UUID) uuid_set.getObject("uuid"));

			ResultSet oid_set = statement.executeQuery("SELECT * FROM " + PostgresqlTypesScenario.OID_ID);

			oid_set.next();
			assertEquals(1234, oid_set.getInt("oid"));

		}
		catch (SQLException throwables) {
			throwables.printStackTrace();
		}
	}
}
