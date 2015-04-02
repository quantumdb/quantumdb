package io.quantumdb.core.backends.postgresql.integration.videostores;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.floats;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;

import java.sql.SQLException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.backends.guice.PersistenceModule;
import io.quantumdb.core.backends.postgresql.PostgresqlDatabase;
import io.quantumdb.core.backends.postgresql.migrator.TableCreator;
import io.quantumdb.core.migration.Migrator;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import lombok.Getter;

@Getter
public class PostgresqlBaseScenario extends PostgresqlDatabase {

	private Backend backend;
	private Catalog catalog;
	private Migrator migrator;
	private Injector injector;
	private Changelog changelog;
	private TableMapping tableMapping;
	private State state;

	@Override
	public void before() throws SQLException, MigrationException, ClassNotFoundException {
		super.before();

		TableCreator tableCreator = new TableCreator();

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
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table payments = new Table("payments")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("staff_id", integer()))
				.addColumn(new Column("customer_id", integer(), NOT_NULL))
				.addColumn(new Column("rental_id", integer(), NOT_NULL))
				.addColumn(new Column("date", date(), NOT_NULL))
				.addColumn(new Column("amount", floats(), NOT_NULL));

		Table rentals = new Table("rentals")
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

		Set<Table> tables = Sets.newHashSet(stores, staff, customers, films, inventory, paychecks, payments, rentals);

		catalog = new Catalog(getCatalogName());
		tables.forEach(catalog::addTable);

		tableCreator.create(getConnection(), tables);

		String jdbcUrl = getJdbcUrl();
		String catalogName = getCatalogName();
		String jdbcUser = getJdbcUser();
		String jdbcPass = getJdbcPass();
		PersistenceModule module = new PersistenceModule(jdbcUrl, catalogName, jdbcUser, jdbcPass);

		injector = Guice.createInjector(module);
		backend = injector.getInstance(Backend.class);

		state = backend.loadState();
		changelog = state.getChangelog();

		// Register pre-existing tables in root version.
		catalog = state.getCatalog();
		tableMapping = state.getTableMapping();
		for (Table table : catalog.getTables()) {
			tableMapping.set(changelog.getRoot(), table.getName(), table.getName());
		}

		backend.persistState(state);
		migrator = injector.getInstance(Migrator.class);
	}

}
