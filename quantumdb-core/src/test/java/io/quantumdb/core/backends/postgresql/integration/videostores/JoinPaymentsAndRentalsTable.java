package io.quantumdb.core.backends.postgresql.integration.videostores;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.floats;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.operations.SchemaOperations.joinTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class JoinPaymentsAndRentalsTable {

	@ClassRule
	public static PostgresqlBaseScenario setup = new PostgresqlBaseScenario();

	private static State state;
	private static Version origin;
	private static Version target;

	@BeforeClass
	public static void performEvolution() throws SQLException, MigrationException {
		setup.insertTestData();

		origin = setup.getChangelog().getLastAdded();

		setup.getChangelog().addChangeSet("Michael de Jong",
				joinTable("payments", "p", "id", "staff_id", "customer_id", "amount")
					.with("rentals", "r", "p.rental_id = r.id", "date", "inventory_id")
					.into("payed_rentals"));

		target = setup.getChangelog().getLastAdded();
		setup.getBackend().persistState(setup.getState());

		setup.getMigrator().migrate(origin.getId(), target.getId());

		state = setup.getBackend().loadState();
	}

	@Test
	public void verifyTableStructure() {
		TableMapping mapping = state.getTableMapping();

		// Original tables and foreign keys.

		Table stores = new Table(mapping.getTableId(origin, "stores"))
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("manager_id", integer(), NOT_NULL));

		Table staff = new Table(mapping.getTableId(origin, "staff"))
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL));

		Table customers = new Table(mapping.getTableId(origin, "customers"))
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("referred_by", integer()));

		Table films = new Table(mapping.getTableId(origin, "films"))
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table inventory = new Table(mapping.getTableId(origin, "inventory"))
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("film_id", integer(), NOT_NULL));

		Table paychecks = new Table(mapping.getTableId(origin, "paychecks"))
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table payments = new Table(mapping.getTableId(origin, "payments"))
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("rental_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table rentals = new Table(mapping.getTableId(origin, "rentals"))
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("inventory_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL));

		stores.addForeignKey("manager_id").referencing(staff, "id");
		staff.addForeignKey("store_id").referencing(stores, "id");
		customers.addForeignKey("referred_by").referencing(customers, "id");
		customers.addForeignKey("store_id").referencing(stores, "id");
		inventory.addForeignKey("store_id").referencing(stores, "id");
		inventory.addForeignKey("film_id").referencing(films, "id");
		paychecks.addForeignKey("staff_id").referencing(staff, "id");
		payments.addForeignKey("staff_id").referencing(staff, "id");
		payments.addForeignKey("customer_id").referencing(customers, "id");
		payments.addForeignKey("rental_id").referencing(rentals, "id");
		rentals.addForeignKey("staff_id").referencing(staff, "id");
		rentals.addForeignKey("customer_id").referencing(customers, "id");
		rentals.addForeignKey("inventory_id").referencing(inventory, "id");

		// New tables and foreign keys.

		Table payedRentals = new Table(mapping.getTableId(target, "payed_rentals"))
				.addColumn(new Column("id", integer(), payments.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("inventory_id", integer(), NOT_NULL));

		payedRentals.addForeignKey("staff_id").referencing(staff, "id");
		payedRentals.addForeignKey("customer_id").referencing(customers, "id");
		payedRentals.addForeignKey("inventory_id").referencing(inventory, "id");

		List<Table> tables = Lists.newArrayList(stores, staff, customers, films, inventory, paychecks, payments, rentals,
				payedRentals);

		Catalog expected = new Catalog(setup.getCatalogName());
		tables.forEach(expected::addTable);

		assertEquals(expected.getTables(), state.getCatalog().getTables());
	}

	@Test
	public void verifyTableMappings() {
		TableMapping tableMapping = state.getTableMapping();

		// Unchanged tables
		assertEquals(PostgresqlBaseScenario.FILMS_ID, tableMapping.getTableId(target, "films"));
		assertEquals(PostgresqlBaseScenario.STORES_ID, tableMapping.getTableId(target, "stores"));
		assertEquals(PostgresqlBaseScenario.STAFF_ID, tableMapping.getTableId(target, "staff"));
		assertEquals(PostgresqlBaseScenario.CUSTOMERS_ID, tableMapping.getTableId(target, "customers"));
		assertEquals(PostgresqlBaseScenario.PAYCHECKS_ID, tableMapping.getTableId(target, "paychecks"));
		assertEquals(PostgresqlBaseScenario.INVENTORY_ID, tableMapping.getTableId(target, "inventory"));
		assertEquals(PostgresqlBaseScenario.RENTALS_ID, tableMapping.getTableId(target, "rentals"));
		assertEquals(PostgresqlBaseScenario.PAYMENTS_ID, tableMapping.getTableId(target, "payments"));
	}

	@Test
	public void verifyContents() throws SQLException {
		String tableName = state.getTableMapping().getTableId(target, "payed_rentals");

		try (Connection connection = setup.getBackend().connect()) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName + " ORDER BY id ASC");

			assertTrue(resultSet.next());
			assertEquals(1, resultSet.getInt("id"));
			assertEquals(1, resultSet.getInt("staff_id"));
			assertEquals(1, resultSet.getInt("customer_id"));
			assertEquals(5f, resultSet.getFloat("amount"));
			assertEquals(1, resultSet.getInt("inventory_id"));

			assertTrue(resultSet.next());
			assertEquals(2, resultSet.getInt("id"));
			assertEquals(2, resultSet.getInt("staff_id"));
			assertEquals(2, resultSet.getInt("customer_id"));
			assertEquals(5f, resultSet.getFloat("amount"));
			assertEquals(2, resultSet.getInt("inventory_id"));

			assertTrue(resultSet.next());
			assertEquals(3, resultSet.getInt("id"));
			assertEquals(3, resultSet.getInt("staff_id"));
			assertEquals(3, resultSet.getInt("customer_id"));
			assertEquals(5f, resultSet.getFloat("amount"));
			assertEquals(5, resultSet.getInt("inventory_id"));

			assertTrue(resultSet.next());
			assertEquals(4, resultSet.getInt("id"));
			assertEquals(null, resultSet.getObject("staff_id"));
			assertEquals(4, resultSet.getInt("customer_id"));
			assertEquals(5f, resultSet.getFloat("amount"));
			assertEquals(6, resultSet.getInt("inventory_id"));

			assertFalse(resultSet.next());
		}
	}

}
