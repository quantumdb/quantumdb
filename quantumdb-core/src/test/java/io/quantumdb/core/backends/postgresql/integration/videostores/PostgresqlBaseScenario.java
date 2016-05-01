package io.quantumdb.core.backends.postgresql.integration.videostores;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.floats;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.backends.postgresql.PostgresqlDatabase;
import io.quantumdb.core.backends.postgresql.migrator.TableCreator;
import io.quantumdb.core.migration.Migrator;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.utils.BatchInserter;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import lombok.Getter;

@Getter
public class PostgresqlBaseScenario extends PostgresqlDatabase {

	public static final String STORES_ID = "table_8e7f3e2bff";
	public static final String STAFF_ID = "table_0bc1cb405a";
	public static final String CUSTOMERS_ID = "table_c5d814a492";
	public static final String FILMS_ID = "table_9fc422e0a8";
	public static final String INVENTORY_ID = "table_61a6a2518f";
	public static final String PAYCHECKS_ID = "table_08f08c873f";
	public static final String PAYMENTS_ID = "table_9859d9b73f";
	public static final String RENTALS_ID = "table_d9cabab994";

	private Backend backend;
	private Catalog catalog;
	private Migrator migrator;
	private Changelog changelog;
	private RefLog refLog;
	private State state;

	@Override
	public void before() throws SQLException, MigrationException, ClassNotFoundException {
		super.before();

		TableCreator tableCreator = new TableCreator();

		Table stores = new Table(STORES_ID)
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("manager_id", integer(), NOT_NULL));

		Table staff = new Table(STAFF_ID)
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL));

		Table customers = new Table(CUSTOMERS_ID)
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("referred_by", integer()));

		Table films = new Table(FILMS_ID)
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table inventory = new Table(INVENTORY_ID)
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("film_id", integer(), NOT_NULL));

		Table paychecks = new Table(PAYCHECKS_ID)
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table payments = new Table(PAYMENTS_ID)
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("rental_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table rentals = new Table(RENTALS_ID)
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("inventory_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL));

		stores.addForeignKey("manager_id").referencing(staff, "id");

		staff.addForeignKey("store_id").referencing(stores, "id");

		customers.addForeignKey("referred_by").referencing(customers, "id");
		customers.addForeignKey("store_id")
				.named("customer_registered_at_store")
				.referencing(stores, "id");

		inventory.addForeignKey("store_id").referencing(stores, "id");
		inventory.addForeignKey("film_id").referencing(films, "id");

		paychecks.addForeignKey("staff_id").referencing(staff, "id");

		payments.addForeignKey("staff_id").referencing(staff, "id");
		payments.addForeignKey("customer_id").referencing(customers, "id");
		payments.addForeignKey("rental_id").referencing(rentals, "id");

		rentals.addForeignKey("staff_id").referencing(staff, "id");
		rentals.addForeignKey("customer_id").referencing(customers, "id");
		rentals.addForeignKey("inventory_id").referencing(inventory, "id");

		Set<Table> tables = Sets.newHashSet(stores, staff, customers, films, inventory, paychecks, payments, rentals);

		catalog = new Catalog(getCatalogName());
		tables.forEach(catalog::addTable);

		tableCreator.create(getConnection(), tables);

		Config config = new Config();
		config.setUrl(getJdbcUrl());
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

		Map<String, String> tableIds = Maps.newHashMap();
		tableIds.put("stores", STORES_ID);
		tableIds.put("staff", STAFF_ID);
		tableIds.put("customers", CUSTOMERS_ID);
		tableIds.put("films", FILMS_ID);
		tableIds.put("inventory", INVENTORY_ID);
		tableIds.put("paychecks", PAYCHECKS_ID);
		tableIds.put("payments", PAYMENTS_ID);
		tableIds.put("rentals", RENTALS_ID);

		catalog.getTables().forEach(table -> {
			refLog.addTable(table.getName(), tableIds.get(table.getName()), changelog.getRoot(), table.getColumns().stream()
					.map(column -> new ColumnRef(column.getName()))
					.collect(Collectors.toList()));
		});

		backend.persistState(state);
		migrator = new Migrator(backend);
	}

	void insertTestData() throws SQLException {
		getConnection().setAutoCommit(false);
		getConnection().createStatement().execute("SET CONSTRAINTS ALL DEFERRED");

		BatchInserter.insertInto(getConnection(), STORES_ID, "name", "manager_id")
				.values("Delft", 1)
				.values("Amsterdam", 2)
				.insert();

		BatchInserter.insertInto(getConnection(), STAFF_ID, "name", "store_id")
				.values("Arend Paulissen", 1)
				.values("Alfred Wauters", 2)
				.values("Damian Roijakkers", 1)
				.values("Anika De Witte", 2)
				.insert();

		BatchInserter.insertInto(getConnection(), PAYCHECKS_ID, "staff_id", "date", "amount")
				.values(1, new Date(System.currentTimeMillis()), 1500f)
				.values(2, new Date(System.currentTimeMillis()), 1500f)
				.values(3, new Date(System.currentTimeMillis()), 1400f)
				.values(4, new Date(System.currentTimeMillis()), 1400f)
				.insert();

		BatchInserter.insertInto(getConnection(), CUSTOMERS_ID, "name", "store_id")
				.values("Johanneke Schoorl", 1)
				.values("Maikel Haanraadts", 1)
				.values("Hubrecht Houtkooper", 2)
				.values("Alex Van Der Aart", 2)
				.insert();

		BatchInserter.insertInto(getConnection(), FILMS_ID, "name")
				.values("Intersteller")
				.values("Gravity")
				.values("Apollo 13")
				.values("Space Odyssey")
				.insert();

		BatchInserter.insertInto(getConnection(), INVENTORY_ID, "store_id", "film_id")
				.values(1, 1)
				.values(1, 2)
				.values(1, 3)
				.values(1, 4)
				.values(2, 1)
				.values(2, 2)
				.values(2, 3)
				.insert();

		BatchInserter.insertInto(getConnection(), RENTALS_ID, "staff_id", "customer_id", "inventory_id", "date")
				.values(1, 1, 1, new Date(System.currentTimeMillis()))
				.values(2, 2, 2, new Date(System.currentTimeMillis()))
				.values(3, 3, 5, new Date(System.currentTimeMillis()))
				.values(null, 4, 6, new Date(System.currentTimeMillis()))
				.insert();

		BatchInserter.insertInto(getConnection(), PAYMENTS_ID, "staff_id", "customer_id", "rental_id", "date", "amount")
				.values(1, 1, 1, new Date(System.currentTimeMillis()), 5f)
				.values(2, 2, 2, new Date(System.currentTimeMillis()), 5f)
				.values(3, 3, 3, new Date(System.currentTimeMillis()), 5f)
				.values(null, 4, 4, new Date(System.currentTimeMillis()), 5f)
				.insert();

		getConnection().setAutoCommit(true);
	}

}
