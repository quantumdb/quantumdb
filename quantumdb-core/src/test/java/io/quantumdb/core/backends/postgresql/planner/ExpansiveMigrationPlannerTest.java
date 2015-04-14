package io.quantumdb.core.backends.postgresql.planner;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.bool;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.floats;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static org.junit.Assert.assertEquals;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class ExpansiveMigrationPlannerTest {

	private State state;
	private Catalog catalog;
	private TableMapping tableMapping;
	private Changelog changelog;
	private MigrationPlanner planner;

	@Before
	public void setUp() {
		this.planner = new ExpansiveMigrationPlanner();
		this.catalog = new Catalog("test-db");

		Table stores = new Table("stores")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("manager_id", integer(), NOT_NULL));

		Table staff = new Table("staff")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL));

		Table customers = new Table("customers")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("referred_by", integer()));

		Table films = new Table("films")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table inventory = new Table("inventory")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("store_id", integer(), NOT_NULL))
				.addColumn(new Column("film_id", integer(), NOT_NULL));

		Table paychecks = new Table("paychecks")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), IDENTITY, NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table payments = new Table("payments")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("rental_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), IDENTITY, NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table rentals = new Table("rentals")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("inventory_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), IDENTITY, NOT_NULL));

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

		catalog.addTable(stores);
		catalog.addTable(staff);
		catalog.addTable(customers);
		catalog.addTable(films);
		catalog.addTable(inventory);
		catalog.addTable(payments);
		catalog.addTable(paychecks);
		catalog.addTable(rentals);

		this.changelog = new Changelog();
		this.tableMapping = TableMapping.bootstrap(changelog.getRoot(), catalog);
		this.state = new State(catalog, tableMapping, changelog);
	}

	@Test
	public void testCorrectMigrationOrderForModifyingStoresTable() {
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("stores", "opened", date(), NOT_NULL));
		MigrationPlan plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		ImmutableList<Step> steps = plan.getSteps();

		List<Step> expectedSteps = Lists.newArrayList();
		expectedSteps.add(new Step(
				new Migration(getTableId("stores"), Sets.newHashSet("manager_id")),
				new Migration(getTableId("staff"), Sets.newHashSet("store_id")),
				new Migration(getTableId("customers"), Sets.newHashSet("referred_by", "store_id"))
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("inventory"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("rentals"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("paychecks"), Sets.newHashSet()),
				new Migration(getTableId("payments"), Sets.newHashSet())
		));

		assertEquals(expectedSteps, steps);
	}

	@Test
	public void testCorrectMigrationOrderForModifyingCustomersTable() {
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("customers", "registered", date(), NOT_NULL));
		MigrationPlan plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		ImmutableList<Step> steps = plan.getSteps();

		List<Step> expectedSteps = Lists.newArrayList();
		expectedSteps.add(new Step(
				new Migration(getTableId("customers"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("rentals"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("payments"), Sets.newHashSet())
		));

		assertEquals(expectedSteps, steps);
	}

	@Test
	public void testCorrectMigrationOrderForModifyingFilmsTable() {
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("films", "released", date(), NOT_NULL));
		MigrationPlan plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		ImmutableList<Step> steps = plan.getSteps();

		List<Step> expectedSteps = Lists.newArrayList();
		expectedSteps.add(new Step(
				new Migration(getTableId("films"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("inventory"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("rentals"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("payments"), Sets.newHashSet())
		));

		assertEquals(expectedSteps, steps);
	}

	@Test
	public void testCorrectMigrationOrderForModifyingInventoryTable() {
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("inventory", "created", date(), NOT_NULL));
		MigrationPlan plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		ImmutableList<Step> steps = plan.getSteps();

		List<Step> expectedSteps = Lists.newArrayList();
		expectedSteps.add(new Step(
				new Migration(getTableId("inventory"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("rentals"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("payments"), Sets.newHashSet())
		));

		assertEquals(expectedSteps, steps);
	}

	@Test
	public void testCorrectMigrationOrderForModifyingRentalsTable() {
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("rentals", "return_date", date(), NOT_NULL));
		MigrationPlan plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		ImmutableList<Step> steps = plan.getSteps();

		List<Step> expectedSteps = Lists.newArrayList();
		expectedSteps.add(new Step(
				new Migration(getTableId("rentals"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("payments"), Sets.newHashSet())
		));

		assertEquals(expectedSteps, steps);
	}

	@Test
	public void testCorrectMigrationOrderForModifyingPaymentsTable() {
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("payments", "checked", bool(), "'false'", NOT_NULL));
		MigrationPlan plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		ImmutableList<Step> steps = plan.getSteps();

		List<Step> expectedSteps = Lists.newArrayList();
		expectedSteps.add(new Step(
				new Migration(getTableId("payments"), Sets.newHashSet())
		));

		assertEquals(expectedSteps, steps);
	}

	@Test
	public void testCorrectMigrationOrderForModifyingStaffTable() {
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("staff", "hired", date(), NOT_NULL));
		MigrationPlan plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		ImmutableList<Step> steps = plan.getSteps();

		List<Step> expectedSteps = Lists.newArrayList();
		expectedSteps.add(new Step(
				new Migration(getTableId("stores"), Sets.newHashSet("manager_id")),
				new Migration(getTableId("staff"), Sets.newHashSet("store_id")),
				new Migration(getTableId("customers"), Sets.newHashSet("store_id", "referred_by"))
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("inventory"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("rentals"), Sets.newHashSet())
		));
		expectedSteps.add(new Step(
				new Migration(getTableId("paychecks"), Sets.newHashSet()),
				new Migration(getTableId("payments"), Sets.newHashSet())
		));

		assertEquals(expectedSteps, steps);
	}

	@Test
	public void testCorrectMigrationOrderForModifyingPaychecksTable() {
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("paychecks", "checked", bool(), "'false'", NOT_NULL));
		MigrationPlan plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		ImmutableList<Step> steps = plan.getSteps();

		List<Step> expectedSteps = Lists.newArrayList();
		expectedSteps.add(new Step(
				new Migration(getTableId("paychecks"), Sets.newHashSet())
		));

		assertEquals(expectedSteps, steps);
	}

	private String getTableId(String tableId) {
		return tableMapping.getTableId(changelog.getLastAdded(), tableId);
	}

}
