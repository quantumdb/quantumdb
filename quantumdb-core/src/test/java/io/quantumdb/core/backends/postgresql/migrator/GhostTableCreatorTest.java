package io.quantumdb.core.backends.postgresql.migrator;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.bigint;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.bool;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.chars;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.doubles;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.floats;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.oid;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.smallint;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.text;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.timestamp;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.uuid;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import io.quantumdb.core.backends.DatabaseMigrator;
import io.quantumdb.core.backends.postgresql.PostgresqlDatabase;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class GhostTableCreatorTest {

	@Rule
	public PostgresqlDatabase setup = new PostgresqlDatabase();

	private GhostTableCreator creator;
	private Catalog catalog;

	@Before
	public void setUp() throws SQLException, ClassNotFoundException {
		this.creator = new GhostTableCreator();
		this.catalog = new Catalog(setup.getCatalogName());
	}

	@Test
	public void testTableWithNonNullableColumns() throws SQLException, DatabaseMigrator.MigrationException {
		Table users = new Table("users")
				.addColumn(new Column("id", integer(), NOT_NULL, IDENTITY))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("age", integer(), NOT_NULL))
				.addColumn(new Column("registered", date(), NOT_NULL));

		catalog.addTable(users);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, users);
	}

	@Test
	public void testTableWithAllTypes() throws SQLException, DatabaseMigrator.MigrationException {
		Table users = new Table("users")
				.addColumn(new Column("id", integer(), NOT_NULL, IDENTITY))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("age", integer(), NOT_NULL))
				.addColumn(new Column("registered", timestamp(false), NOT_NULL))
				.addColumn(new Column("date_of_birth", date(), NOT_NULL))
				.addColumn(new Column("current_balance", floats(), NOT_NULL))
				.addColumn(new Column("star_date", doubles(), NOT_NULL))
				.addColumn(new Column("phone_number", bigint(), NOT_NULL))
				.addColumn(new Column("po_box_number", smallint(), NOT_NULL))
				.addColumn(new Column("remarks", text(), NOT_NULL))
				.addColumn(new Column("gender", chars(1), NOT_NULL))
				.addColumn(new Column("active", bool(), NOT_NULL))
				.addColumn(new Column("uuid", uuid(), NOT_NULL))
				.addColumn(new Column("oid", oid(), NOT_NULL));

		catalog.addTable(users);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, users);
	}

	@Test
	public void testNullableSelfReferencingTable() throws SQLException, DatabaseMigrator.MigrationException {
		Table users = new Table("users")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY))
				.addColumn(new Column("name", varchar(255)))
				.addColumn(new Column("referred_by", integer()));

		users.addForeignKey("referred_by").referencing(users, "id");

		catalog.addTable(users);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, users);
	}

	@Test
	public void testNonNullableSelfReferencingTable() throws SQLException, DatabaseMigrator.MigrationException {
		Table users = new Table("users")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY))
				.addColumn(new Column("name", varchar(255)))
				.addColumn(new Column("referred_by", integer(), NOT_NULL));

		users.addForeignKey("referred_by").referencing(users, "id");

		catalog.addTable(users);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, users);
	}

	@Test
	public void testNonNullableCircleReference() throws SQLException, DatabaseMigrator.MigrationException {
		Table users = new Table("users")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY))
				.addColumn(new Column("account_id", integer(), NOT_NULL));

		Table accounts = new Table("accounts")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY))
				.addColumn(new Column("user_id", integer(), NOT_NULL));

		users.addForeignKey("account_id").referencing(accounts, "id");
		accounts.addForeignKey("user_id").referencing(users, "id");

		catalog.addTable(users);
		catalog.addTable(accounts);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, users, accounts);
	}

	@Test
	public void testNullableReference() throws SQLException, DatabaseMigrator.MigrationException {
		Table rentals = new Table("rentals")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY))
				.addColumn(new Column("payment_id", integer()));

		Table payments = new Table("payments")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY));

		rentals.addForeignKey("payment_id").referencing(payments, "id");

		catalog.addTable(rentals);
		catalog.addTable(payments);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, rentals, payments);
	}

	@Test
	public void testNonNullableReference() throws SQLException, DatabaseMigrator.MigrationException {
		Table rentals = new Table("rentals")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY))
				.addColumn(new Column("payment_id", integer(), NOT_NULL));

		Table payments = new Table("payments")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY));

		rentals.addForeignKey("payment_id").referencing(payments, "id");

		catalog.addTable(rentals);
		catalog.addTable(payments);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, rentals, payments);
	}

	@Test
	public void testCompositeKey() throws SQLException, DatabaseMigrator.MigrationException {
		Table rentals = new Table("rentals")
				.addColumn(new Column("user_id", integer(), IDENTITY, NOT_NULL))
				.addColumn(new Column("film_id", integer(), IDENTITY, NOT_NULL));

		Table users = new Table("users")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY));

		Table films = new Table("films")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY));

		rentals.addForeignKey("user_id").referencing(users, "id");
		rentals.addForeignKey("film_id").referencing(films, "id");

		catalog.addTable(rentals);
		catalog.addTable(users);
		catalog.addTable(films);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, rentals, users, films);
	}

	@Test
	public void testCompositeReference() throws SQLException, DatabaseMigrator.MigrationException {
		Table rentals = new Table("rentals")
				.addColumn(new Column("user_id", integer(), IDENTITY, NOT_NULL))
				.addColumn(new Column("film_id", integer(), IDENTITY, NOT_NULL));

		Table users = new Table("users")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY));

		Table films = new Table("films")
				.addColumn(new Column("id", integer(), AUTO_INCREMENT, NOT_NULL, IDENTITY));

		Table payments = new Table("payments")
				.addColumn(new Column("user_id", integer(), IDENTITY, NOT_NULL))
				.addColumn(new Column("film_id", integer(), IDENTITY, NOT_NULL));

		rentals.addForeignKey("user_id").referencing(users, "id");
		rentals.addForeignKey("film_id").referencing(films, "id");
		payments.addForeignKey("user_id", "film_id").referencing(rentals, "user_id", "film_id");

		catalog.addTable(rentals);
		catalog.addTable(users);
		catalog.addTable(films);
		catalog.addTable(payments);

		Map<Table, Identity> identities = creator.create(setup.getConnection(), catalog.getTables());
		assertIdentitiesPresent(identities, rentals, users, films, payments);
	}

	private void assertIdentitiesPresent(Map<Table, Identity> identities, Table... tables) throws SQLException {
		Set<Table> toCheck = Sets.newHashSet(tables);

		for (Entry<Table, Identity> entry : identities.entrySet()) {
			Table table = entry.getKey();
			Identity identity = entry.getValue();

			if (!toCheck.contains(table)) {
				assertEquals(0, countRows(table));
				continue;
			}

			assertEquals(1, countRows(table));
			Set<String> identityColumns = table.getIdentityColumns().stream()
					.map(Column::getName)
					.collect(Collectors.toSet());

			assertEquals(identityColumns, identity.keys());
		}
	}

	private int countRows(Table table) throws SQLException {
		Statement statement = setup.getConnection().createStatement();
		ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) as cnt FROM " + table.getName());
		if (resultSet.next()) {
			return resultSet.getInt("cnt");
		}
		return 0;
	}

}
