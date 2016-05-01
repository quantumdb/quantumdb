package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.text;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.backends.postgresql.PostgresTypes;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Changelog;
import org.junit.Before;
import org.junit.Test;

public class AlterColumnMigratorTest {

	private RefLog refLog;
	private Catalog catalog;
	private Changelog changelog;
	private AlterColumnMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db")
				.addTable(new Table("users")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("name", varchar(255), NOT_NULL)))
				.addTable(new Table("referrals")
						.addColumn(new Column("invitee_id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("invited_by_id", integer())));

		this.changelog = new Changelog();
		this.refLog = RefLog.init(catalog, changelog.getRoot());

		this.migrator = new AlterColumnMigrator();
	}

	@Test
	public void testExpandForRenamingColumn() {
		AlterColumn operation = SchemaOperations.alterColumn("users", "name").rename("full_name");
		changelog.addChangeSet("Michael de Jong", "Renaming 'name' column to 'full_name' column.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("users");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("full_name", varchar(255), NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForChangingTypeOfColumn() {
		AlterColumn operation = SchemaOperations.alterColumn("users", "name").modifyDataType(PostgresTypes.text());
		changelog.addChangeSet("Michael de Jong", "Set type of 'name' column to 'text'.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("users");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("name", text(), NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForSettingDefaultValue() {
		AlterColumn operation = SchemaOperations.alterColumn("users", "name").modifyDefaultExpression("'Unknown'");
		changelog.addChangeSet("Michael de Jong", "Set default of 'name' column to 'Unknown'.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("users");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("name", varchar(255), "'Unknown'", NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForAlteringDefaultValue() {
		testExpandForSettingDefaultValue();

		AlterColumn operation = SchemaOperations.alterColumn("users", "name").modifyDefaultExpression("'John Smith'");
		changelog.addChangeSet("Michael de Jong", "Set default of 'name' column to 'John Smith'.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("users");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("name", varchar(255), "'John Smith'", NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForRemovingDefaultValue() {
		testExpandForSettingDefaultValue();

		AlterColumn operation = SchemaOperations.alterColumn("users", "name").dropDefaultExpression();
		changelog.addChangeSet("Michael de Jong", "Set default of 'name' column to 'John Smith'.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("users");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForAddingNotNullHint() {
		AlterColumn operation = SchemaOperations.alterColumn("referrals", "invited_by_id").addHint(NOT_NULL);
		changelog.addChangeSet("Michael de Jong", "Added NOT_NULL constraint to 'invited_by_id' column.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("referrals");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("invitee_id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("invited_by_id", integer(), NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForRemovingNotNullHint() {
		AlterColumn operation = SchemaOperations.alterColumn("referrals", "invitee_id").dropHint(NOT_NULL);
		changelog.addChangeSet("Michael de Jong", "Dropped NOT_NULL constraint of 'invitee_id' column.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("referrals");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("invitee_id", integer(), IDENTITY, AUTO_INCREMENT))
				.addColumn(new Column("invited_by_id", integer()));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForAddingAutoIncrementHint() {
		AlterColumn operation = SchemaOperations.alterColumn("referrals", "invited_by_id").addHint(AUTO_INCREMENT);
		changelog.addChangeSet("Michael de Jong", "Added AUTO_INCREMENT constraint to 'invited_by_id' column.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("referrals");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("invitee_id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("invited_by_id", integer(), AUTO_INCREMENT));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForRemovingAutoIncrementHint() {
		AlterColumn operation = SchemaOperations.alterColumn("referrals", "invitee_id").dropHint(AUTO_INCREMENT);
		changelog.addChangeSet("Michael de Jong", "Dropped NOT_NULL constraint of 'invitee_id' column.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("referrals");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("invitee_id", integer(), IDENTITY, NOT_NULL))
				.addColumn(new Column("invited_by_id", integer()));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForAddingIdentityHint() {
		AlterColumn operation = SchemaOperations.alterColumn("referrals", "invited_by_id").addHint(IDENTITY);
		changelog.addChangeSet("Michael de Jong", "Added IDENTITY constraint to 'invited_by_id' column.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("referrals");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("invitee_id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("invited_by_id", integer(), IDENTITY));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForRemovingIdentityHint() {
		testExpandForAddingIdentityHint();

		AlterColumn operation = SchemaOperations.alterColumn("referrals", "invitee_id").dropHint(IDENTITY);
		changelog.addChangeSet("Michael de Jong", "Dropped IDENTITY constraint of 'invitee_id' column.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("referrals");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("invitee_id", integer(), AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("invited_by_id", integer(), IDENTITY));

		assertEquals(expectedGhostTable, ghostTable);
	}

	private Table getGhostTable(Table table) {
		TableRef tableRef = refLog.getTableRef(changelog.getLastAdded(), table.getName());
		String tableId = tableRef.getTableId();
		return catalog.getTable(tableId);
	}

}
