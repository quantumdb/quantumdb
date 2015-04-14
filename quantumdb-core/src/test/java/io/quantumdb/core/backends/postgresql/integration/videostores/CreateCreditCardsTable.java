package io.quantumdb.core.backends.postgresql.integration.videostores;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.floats;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
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
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class CreateCreditCardsTable {

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
				SchemaOperations.createTable("credit_cards")
						.with("credit_card_number", varchar(255), IDENTITY, NOT_NULL)
						.with("card_holder_name", varchar(255), NOT_NULL)
						.with("expiration_date", date(), NOT_NULL));

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

		Table creditCards = new Table(mapping.getTableId(target, "credit_cards"))
				.addColumn(new Column("credit_card_number", varchar(255), IDENTITY, NOT_NULL))
				.addColumn(new Column("card_holder_name", varchar(255), NOT_NULL))
				.addColumn(new Column("expiration_date", date(), NOT_NULL));

		List<Table> tables = Lists.newArrayList(stores, staff, customers, films, inventory, paychecks, payments, rentals,
				creditCards);

		Catalog expected = new Catalog(setup.getCatalogName());
		tables.forEach(expected::addTable);

		assertEquals(expected.getTables(), state.getCatalog().getTables());
	}

	@Test
	public void verifyTableMappings() {
		TableMapping tableMapping = state.getTableMapping();

		// Unchanged tables
		assertEquals(PostgresqlBaseScenario.FILMS_ID, tableMapping.getTableId(target, "films"));
		assertEquals(PostgresqlBaseScenario.CUSTOMERS_ID, tableMapping.getTableId(target, "customers"));
		assertEquals(PostgresqlBaseScenario.PAYMENTS_ID, tableMapping.getTableId(target, "payments"));
		assertEquals(PostgresqlBaseScenario.RENTALS_ID, tableMapping.getTableId(target, "rentals"));
		assertEquals(PostgresqlBaseScenario.STORES_ID, tableMapping.getTableId(target, "stores"));
		assertEquals(PostgresqlBaseScenario.STAFF_ID, tableMapping.getTableId(target, "staff"));
		assertEquals(PostgresqlBaseScenario.INVENTORY_ID, tableMapping.getTableId(target, "inventory"));
		assertEquals(PostgresqlBaseScenario.PAYCHECKS_ID, tableMapping.getTableId(target, "paychecks"));

		// New tables
		assertNotNull(tableMapping.getTableId(target, "credit_cards"));
	}

}
