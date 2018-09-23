package io.quantumdb.core.backends.integration.videostores;

import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.CUSTOMERS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.FILMS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.INVENTORY_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.PAYCHECKS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.PAYMENTS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.RENTALS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.STAFF_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.STORES_ID;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.PostgresTypes.date;
import static io.quantumdb.core.schema.definitions.PostgresTypes.floats;
import static io.quantumdb.core.schema.definitions.PostgresTypes.integer;
import static io.quantumdb.core.schema.definitions.PostgresTypes.varchar;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class RenameCustomersTable {

	@ClassRule
	public static PostgresqlBaseScenario setup = new PostgresqlBaseScenario();

	private static State state;
	private static Version origin;
	private static Version target;

	@BeforeClass
	public static void performEvolution() throws SQLException, MigrationException {
		origin = setup.getChangelog().getLastAdded();

		setup.getChangelog().addChangeSet("test", "Michael de Jong",
				SchemaOperations.renameTable("customers", "clients"));

		target = setup.getChangelog().getLastAdded();
		setup.getBackend().persistState(setup.getState());

		setup.getMigrator().migrate(origin.getId(), target.getId());

		state = setup.getBackend().loadState();
	}

	@Test
	public void verifyTableStructure() {
		RefLog refLog = state.getRefLog();

		Table stores = new Table(refLog.getTableRef(origin, "stores").getRefId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("manager_id", integer(), NOT_NULL));

		Table staff = new Table(refLog.getTableRef(origin, "staff").getRefId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL));

		Table customers = new Table(refLog.getTableRef(origin, "customers").getRefId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("referred_by", integer()));

		Table films = new Table(refLog.getTableRef(origin, "films").getRefId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table inventory = new Table(refLog.getTableRef(origin, "inventory").getRefId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("film_id", integer(), NOT_NULL));

		Table paychecks = new Table(refLog.getTableRef(origin, "paychecks").getRefId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table payments = new Table(refLog.getTableRef(origin, "payments").getRefId())
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("rental_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table rentals = new Table(refLog.getTableRef(origin, "rentals").getRefId())
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

		List<Table> tables = Lists.newArrayList(stores, staff, customers, films, inventory, paychecks, payments, rentals);

		Catalog expected = new Catalog(setup.getCatalogName());
		tables.forEach(expected::addTable);

		assertEquals(expected.getTables(), state.getCatalog().getTables());
	}

	@Test
	public void verifyTableMappings() {
		RefLog refLog = state.getRefLog();

		Map<String, String> originRefIds = refLog.getTableRefs(origin).stream()
				.collect(Collectors.toMap(TableRef::getRefId, TableRef::getName));

		Map<String, String> targetRefIds = refLog.getTableRefs(target).stream()
				.collect(Collectors.toMap(TableRef::getRefId, TableRef::getName));

		Map<String, String> expectedOriginRefIds = ImmutableMap.<String, String>builder()
				.put(STORES_ID, "stores")
				.put(STAFF_ID, "staff")
				.put(CUSTOMERS_ID, "customers")
				.put(FILMS_ID, "films")
				.put(INVENTORY_ID, "inventory")
				.put(PAYCHECKS_ID, "paychecks")
				.put(PAYMENTS_ID, "payments")
				.put(RENTALS_ID, "rentals")
				.build();

		Map<String, String>expectedTargetRefIds = ImmutableMap.<String, String>builder()
				.put(STORES_ID, "stores")
				.put(STAFF_ID, "staff")
				.put(CUSTOMERS_ID, "clients")
				.put(FILMS_ID, "films")
				.put(INVENTORY_ID, "inventory")
				.put(PAYCHECKS_ID, "paychecks")
				.put(PAYMENTS_ID, "payments")
				.put(RENTALS_ID, "rentals")
				.build();

		assertEquals(expectedOriginRefIds, originRefIds);
		assertEquals(expectedTargetRefIds, targetRefIds);
	}

}
