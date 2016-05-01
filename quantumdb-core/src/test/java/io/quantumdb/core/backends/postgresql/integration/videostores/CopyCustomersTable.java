package io.quantumdb.core.backends.postgresql.integration.videostores;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.floats;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.backends.postgresql.integration.videostores.PostgresqlBaseScenario.CUSTOMERS_ID;
import static io.quantumdb.core.backends.postgresql.integration.videostores.PostgresqlBaseScenario.FILMS_ID;
import static io.quantumdb.core.backends.postgresql.integration.videostores.PostgresqlBaseScenario.INVENTORY_ID;
import static io.quantumdb.core.backends.postgresql.integration.videostores.PostgresqlBaseScenario.PAYCHECKS_ID;
import static io.quantumdb.core.backends.postgresql.integration.videostores.PostgresqlBaseScenario.PAYMENTS_ID;
import static io.quantumdb.core.backends.postgresql.integration.videostores.PostgresqlBaseScenario.RENTALS_ID;
import static io.quantumdb.core.backends.postgresql.integration.videostores.PostgresqlBaseScenario.STAFF_ID;
import static io.quantumdb.core.backends.postgresql.integration.videostores.PostgresqlBaseScenario.STORES_ID;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class CopyCustomersTable {

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
				SchemaOperations.copyTable("customers", "customers_backup"));

		target = setup.getChangelog().getLastAdded();
		setup.getBackend().persistState(setup.getState());

		setup.getMigrator().migrate(origin.getId(), target.getId());

		state = setup.getBackend().loadState();
	}

	@Test
	public void verifyTableStructure() {
		RefLog refLog = state.getRefLog();

		// Original tables and foreign keys.

		Table stores = new Table(refLog.getTableRef(origin, "stores").getTableId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("manager_id", integer(), NOT_NULL));

		Table staff = new Table(refLog.getTableRef(origin, "staff").getTableId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL));

		Table customers = new Table(refLog.getTableRef(origin, "customers").getTableId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("referred_by", integer()));

		Table films = new Table(refLog.getTableRef(origin, "films").getTableId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table inventory = new Table(refLog.getTableRef(origin, "inventory").getTableId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("film_id", integer(), NOT_NULL));

		Table paychecks = new Table(refLog.getTableRef(origin, "paychecks").getTableId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table payments = new Table(refLog.getTableRef(origin, "payments").getTableId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("rental_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table rentals = new Table(refLog.getTableRef(origin, "rentals").getTableId())
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

		Table newCustomers = new Table(refLog.getTableRef(target, "customers_backup").getTableId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("referred_by", integer()));

		newCustomers.addForeignKey("referred_by").referencing(newCustomers, "id");
		newCustomers.addForeignKey("store_id").referencing(stores, "id");

		List<Table> tables = Lists.newArrayList(stores, staff, customers, films, inventory, paychecks, payments, rentals,
				newCustomers);

		Catalog expected = new Catalog(setup.getCatalogName());
		tables.forEach(expected::addTable);

		assertEquals(expected.getTables(), state.getCatalog().getTables());
	}

	@Test
	public void verifyTableMappings() {
		RefLog refLog = state.getRefLog();

		// Unchanged tables
		assertEquals(FILMS_ID, refLog.getTableRef(target, "films").getTableId());
		assertEquals(CUSTOMERS_ID, refLog.getTableRef(target, "customers").getTableId());
		assertEquals(PAYMENTS_ID, refLog.getTableRef(target, "payments").getTableId());
		assertEquals(RENTALS_ID, refLog.getTableRef(target, "rentals").getTableId());
		assertEquals(STORES_ID, refLog.getTableRef(target, "stores").getTableId());
		assertEquals(STAFF_ID, refLog.getTableRef(target, "staff").getTableId());
		assertEquals(INVENTORY_ID, refLog.getTableRef(target, "inventory").getTableId());
		assertEquals(PAYCHECKS_ID, refLog.getTableRef(target, "paychecks").getTableId());

		// New tables
		assertNotNull(refLog.getTableRef(target, "customers_backup").getTableId());
	}

}
