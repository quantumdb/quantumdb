package io.quantumdb.core.backends.postgresql.integration.videostores;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.floats;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
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

public class AddColumnToCustomersTable {

	@ClassRule
	public static PostgresqlBaseScenario setup = new PostgresqlBaseScenario();

	private static State state;
	private static Version origin;
	private static Version target;

	@BeforeClass
	public static void performEvolution() throws SQLException, MigrationException {
		origin = setup.getChangelog().getLastAdded();

		setup.getChangelog().addChangeSet("Michael de Jong",
				SchemaOperations.addColumn("customers", "date_of_birth", date()));

		target = setup.getChangelog().getLastAdded();
		setup.getBackend().persistState(setup.getState());

		setup.getMigrator().migrate(origin.getId(), target.getId());

		state = setup.getBackend().loadState();
	}

	/*
	 * Using the expansive method of forking the schema, this test should result in two completely separated
	 * database schemas. The schema associated with the original version, should contain all original tables,
	 * while the schema associated with the new version, should contain ghost tables of all original tables.
	 * There should be no foreign key linking tables from the old schema to the new schema or vice-versa.
	 */
	@Test
	public void verifyStoresTableStructure() {
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

		Table newStores = new Table(mapping.getTableId(target, "stores"))
				.addColumn(new Column("id", integer(), stores.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("manager_id", integer(), NOT_NULL));

		Table newStaff = new Table(mapping.getTableId(target, "staff"))
				.addColumn(new Column("id", integer(), staff.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL));

		Table newCustomers = new Table(mapping.getTableId(target, "customers"))
				.addColumn(new Column("id", integer(), customers.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("referred_by", integer()))
				.addColumn(new Column("date_of_birth", date()));

		Table newFilms = new Table(mapping.getTableId(target, "films"))
				.addColumn(new Column("id", integer(), films.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table newInventory = new Table(mapping.getTableId(target, "inventory"))
				.addColumn(new Column("id", integer(), inventory.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("film_id", integer(), NOT_NULL));

		Table newPaychecks = new Table(mapping.getTableId(target, "paychecks"))
				.addColumn(new Column("id", integer(), paychecks.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table newPayments = new Table(mapping.getTableId(target, "payments"))
				.addColumn(new Column("id", integer(), payments.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("rental_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table newRentals = new Table(mapping.getTableId(target, "rentals"))
				.addColumn(new Column("id", integer(), rentals.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("inventory_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL));

		newStores.addForeignKey("manager_id").referencing(newStaff, "id");
		newStaff.addForeignKey("store_id").referencing(newStores, "id");
		newCustomers.addForeignKey("referred_by").referencing(newCustomers, "id");
		newCustomers.addForeignKey("store_id").referencing(newStores, "id");
		newInventory.addForeignKey("store_id").referencing(newStores, "id");
		newInventory.addForeignKey("film_id").referencing(newFilms, "id");
		newPaychecks.addForeignKey("staff_id").referencing(newStaff, "id");
		newPayments.addForeignKey("staff_id").referencing(newStaff, "id");
		newPayments.addForeignKey("customer_id").referencing(newCustomers, "id");
		newPayments.addForeignKey("rental_id").referencing(newRentals, "id");
		newRentals.addForeignKey("staff_id").referencing(newStaff, "id");
		newRentals.addForeignKey("customer_id").referencing(newCustomers, "id");
		newRentals.addForeignKey("inventory_id").referencing(newInventory, "id");

		Set<Table> tables = Sets.newHashSet(stores, staff, customers, films, inventory, paychecks, payments, rentals,
				newStores, newStaff, newCustomers, newFilms, newInventory, newPaychecks, newPayments, newRentals);

		Catalog expected = new Catalog(setup.getCatalogName());
		tables.forEach(expected::addTable);

		assertEquals(expected.getTables(), state.getCatalog().getTables());
	}

	@Test
	public void verifyTableMappings() {
		TableMapping tableMapping = state.getTableMapping();

		Set<String> originTableIds = tableMapping.getTableIds(origin);
		Set<String> targetTableIds = tableMapping.getTableIds(target);

		Set<String> expectedOriginTableIds = Sets.newHashSet(
				"stores", "staff", "customers", "films",
				"inventory", "paychecks", "payments", "rentals");

		Set<String> expectedTargetTableIds = expectedOriginTableIds.stream()
				.map(tableId -> {
					String tableName = tableMapping.getTableName(origin, tableId);
					return tableMapping.getTableId(target, tableName);
				})
				.collect(Collectors.toSet());

		assertEquals(expectedOriginTableIds, originTableIds);
		assertEquals(expectedTargetTableIds, targetTableIds);
		assertTrue(Sets.intersection(originTableIds, targetTableIds).isEmpty());
	}

}
