package io.quantumdb.core.backends.integration.types;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.Column.Hint.PRIMARY_KEY;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bigserial;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bool;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bytea;
import static io.quantumdb.core.schema.definitions.PostgresTypes.chars;
import static io.quantumdb.core.schema.definitions.PostgresTypes.date;
import static io.quantumdb.core.schema.definitions.PostgresTypes.doubles;
import static io.quantumdb.core.schema.definitions.PostgresTypes.floats;
import static io.quantumdb.core.schema.definitions.PostgresTypes.integer;
import static io.quantumdb.core.schema.definitions.PostgresTypes.numeric;
import static io.quantumdb.core.schema.definitions.PostgresTypes.oid;
import static io.quantumdb.core.schema.definitions.PostgresTypes.serial;
import static io.quantumdb.core.schema.definitions.PostgresTypes.smallint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.smallserial;
import static io.quantumdb.core.schema.definitions.PostgresTypes.text;
import static io.quantumdb.core.schema.definitions.PostgresTypes.timestamp;
import static io.quantumdb.core.schema.definitions.PostgresTypes.uuid;
import static io.quantumdb.core.schema.definitions.PostgresTypes.varchar;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.backends.PostgresqlDatabase;
import io.quantumdb.core.migration.Migrator;
import io.quantumdb.core.planner.TableCreator;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.BatchInserter;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.State;
import lombok.Getter;

@Getter
public class PostgresqlTypesScenario extends PostgresqlDatabase {

	public static final String NUMERIC_SMALL_ID = "table_bb4afbec1e";
	public static final String NUMERIC_ID = "table_968ee03f3c";
	public static final String NUMERIC_BIG_ID = "table_582b679a79";
	public static final String CHARACTER_ID = "table_0137af8ae7";
	public static final String BINARY_ID = "table_ad471f5e8b";
	public static final String DATE_ID = "table_588068de2b";
	public static final String BOOLEAN_ID = "table_81462aed99";
	public static final String UUID_ID = "table_5e6d789720";
	public static final String OID_ID = "table_84ec85b580";


	private Backend backend;
	private Catalog catalog;
	private Migrator migrator;
	private Changelog changelog;
	private RefLog refLog;
	private State state;

	@Override
	public void before() throws SQLException, MigrationException, ClassNotFoundException {
		super.before();

		TableCreator tableCreator = new TableCreator(getConfig());

		Table numeric_small = new Table(NUMERIC_SMALL_ID)
				// Serial will automatically create a sequence (AUTO_INCREMENT) and NOT_NULL constraint
				.addColumn(new Column("id", smallserial(), PRIMARY_KEY))
				.addColumn(new Column("smallint", smallint(), NOT_NULL))
				.addColumn(new Column("smallnumeric_integer", numeric(5, 0), NOT_NULL))
				.addColumn(new Column("smallnumeric_decimal", numeric(5, 2), NOT_NULL))
				.addColumn(new Column("smallnumeric", numeric(5), NOT_NULL));

		Table numeric = new Table(NUMERIC_ID)
				.addColumn(new Column("id", serial(), PRIMARY_KEY))
				.addColumn(new Column("int", integer(), NOT_NULL))
				.addColumn(new Column("float", floats(), NOT_NULL))
				.addColumn(new Column("double", doubles(), NOT_NULL))
				.addColumn(new Column("numeric_integer", numeric(10, 0), NOT_NULL))
				.addColumn(new Column("numeric_decimal", numeric(10, 5), NOT_NULL))
				.addColumn(new Column("numeric", numeric(10), NOT_NULL));

		Table numeric_big = new Table(NUMERIC_BIG_ID)
				.addColumn(new Column("id", bigserial(), PRIMARY_KEY))
				.addColumn(new Column("bigint", bigint(), NOT_NULL))
				.addColumn(new Column("bignumeric_integer", numeric(1000, 0), NOT_NULL))
				.addColumn(new Column("bignumeric_decimal", numeric(1000, 100), NOT_NULL))
				.addColumn(new Column("bignumeric", numeric(), NOT_NULL));

		Table character = new Table(CHARACTER_ID)
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("varchar_unlimited", varchar(), NOT_NULL))
				.addColumn(new Column("varchar_10", varchar(10), NOT_NULL))
				.addColumn(new Column("varchar_100", varchar(100), NOT_NULL))
				.addColumn(new Column("char", chars(1), NOT_NULL))
				.addColumn(new Column("char_100", chars(100), NOT_NULL))
				.addColumn(new Column("text", text(), NOT_NULL));

		Table binary = new Table(BINARY_ID)
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("bytea", bytea(), NOT_NULL));

		Table date = new Table(DATE_ID)
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("timestamp_0", timestamp(false, 0), NOT_NULL))
				.addColumn(new Column("timestamp_6", timestamp(false, 6), NOT_NULL))
				.addColumn(new Column("timestamptz_0", timestamp(true, 0), NOT_NULL))
				.addColumn(new Column("timestamptz_6", timestamp(false, 6), NOT_NULL));

		Table bool = new Table(BOOLEAN_ID)
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("bool", bool(), NOT_NULL));

		Table uuid = new Table(UUID_ID)
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("uuid", uuid(), NOT_NULL));

		Table oid = new Table(OID_ID)
				.addColumn(new Column("id", bigint(), PRIMARY_KEY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("oid", oid(), NOT_NULL));

		Set<Table> tables = Sets.newHashSet(numeric_small, numeric, numeric_big, character, binary, date, bool, uuid, oid);

		catalog = new Catalog(getCatalogName());
		tables.forEach(catalog::addTable);

		tableCreator.create(getConnection(), tables);

		Config config = new Config();
		config.setUrl(getJdbcUrl() + "/" + getCatalogName());
		config.setUser(getJdbcUser());
		config.setPassword(getJdbcPass());
		config.setCatalog(getCatalogName());
		config.setDriver(getJdbcDriver());

		backend = config.getBackend();

		state = backend.loadState();
		changelog = state.getChangelog();

		// Register pre-existing tables in root version.
		catalog = state.getCatalog();
		refLog = state.getRefLog();

		Map<String, String> refIds = Maps.newHashMap();
		refIds.put("numeric_small", NUMERIC_SMALL_ID);
		refIds.put("numeric", NUMERIC_ID);
		refIds.put("numeric_big", NUMERIC_BIG_ID);
		refIds.put("character", CHARACTER_ID);
		refIds.put("binary", BINARY_ID);
		refIds.put("date", DATE_ID);
		refIds.put("bool", BOOLEAN_ID);
		refIds.put("uuid", UUID_ID);
		refIds.put("oid", OID_ID);

		refIds.forEach((tableName, refId) -> {
			Table table = catalog.getTable(refId);
			List<ColumnRef> columns = table.getColumns().stream()
					.map(column -> new ColumnRef(column.getName()))
					.collect(Collectors.toList());

			refLog.addTable(tableName, refId, changelog.getRoot(), columns);
		});

		backend.persistState(state, null);
		migrator = new Migrator(backend);
	}


	public void insertTestData() throws SQLException {
		getConnection().setAutoCommit(false);
		getConnection().createStatement().execute("SET CONSTRAINTS ALL DEFERRED");

		BatchInserter.insertInto(getConnection(), NUMERIC_SMALL_ID, "smallint", "smallnumeric_integer", "smallnumeric_decimal", "smallnumeric")
				.values(1, new BigDecimal("1"), new BigDecimal("123.45"), new BigDecimal("12345"))
				.values(2, new BigDecimal("2"), new BigDecimal("123.456"), new BigDecimal("1234.5"))
				.insert();

		BatchInserter.insertInto(getConnection(), NUMERIC_ID, "int", "float", "double", "numeric_integer", "numeric_decimal", "numeric")
				.values(1, 1.23456f, 1.234567890123456d, new BigDecimal("1000000000"), new BigDecimal("1234.567894"), new BigDecimal("123.9"))
				.values(2, 123f, 123d, new BigDecimal("123"), new BigDecimal("123"), new BigDecimal("123"))
				.insert();

		BatchInserter.insertInto(getConnection(), NUMERIC_BIG_ID, "bigint", "bignumeric_integer", "bignumeric_decimal", "bignumeric")
				.values(9223372036854775807L, new BigDecimal("1234567890123456789012345678901234567890"),
						new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789012345678901234567890"),
						new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789012345678901234567890"))
				.insert();

		BatchInserter.insertInto(getConnection(), CHARACTER_ID, "varchar_unlimited", "varchar_10", "varchar_100", "char", "char_100", "text")
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

		BatchInserter.insertInto(getConnection(), BINARY_ID, "bytea")
				.values("pdf_file".getBytes())
				.insert();

		Timestamp timestamp = new Timestamp(1600000000123L);
		timestamp.setNanos(123456789);
		BatchInserter.insertInto(getConnection(), DATE_ID, "date", "timestamp_0", "timestamp_6", "timestamptz_0", "timestamptz_6")
				.values(new Date(1600000000123L), timestamp,
						timestamp, timestamp,
						timestamp)
				.insert();

		BatchInserter.insertInto(getConnection(), BOOLEAN_ID, "bool")
				.values(true)
				.values(false)
				.insert();

		BatchInserter.insertInto(getConnection(), UUID_ID, "uuid")
				.values(UUID.fromString("4cc75deb-cd5f-49e8-b94c-c692103e370c"))
				.insert();

		BatchInserter.insertInto(getConnection(), OID_ID, "oid")
				.values(1234)
				.insert();

		getConnection().setAutoCommit(true);
	}

}
