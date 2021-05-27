package io.quantumdb.core.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.PostgresqlDatabase;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Index;
import io.quantumdb.core.schema.definitions.PostgresTypes;
import io.quantumdb.core.schema.definitions.Table;
import org.junit.Rule;
import org.junit.Test;

public class TableCreatorTest {

	@Rule
	public final PostgresqlDatabase database = new PostgresqlDatabase();

	@Test
	public void testCreatingSimpleTable() throws SQLException {
		try (Connection connection = database.createConnection()) {
			Catalog catalog = new Catalog(database.getCatalogName());
			Table users = new Table("users")
					.addColumn(new Column("id", PostgresTypes.bigint(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
					.addColumn(new Column("name", PostgresTypes.text(), Hint.NOT_NULL));

			catalog.addTable(users);

			TableCreator tableCreator = new TableCreator(database.getConfig());
			tableCreator.create(connection, Lists.newArrayList(users));
		}

		try (Connection connection = database.createConnection()) {
			Catalog catalog = CatalogLoader.load(connection, database.getCatalogName());
			Table users = catalog.getTable("users");

			Column id = users.getColumn("id");
			assertEquals(Sets.newHashSet(Hint.PRIMARY_KEY, Hint.NOT_NULL, Hint.AUTO_INCREMENT), id.getHints());
			assertEquals(PostgresTypes.bigint(), id.getType());
			assertEquals("users_id_seq", id.getSequence().getName());
			assertNull(id.getDefaultValue());
			assertNull(id.getOutgoingForeignKey());

			Column name = users.getColumn("name");
			assertEquals(Sets.newHashSet(Hint.NOT_NULL), name.getHints());
			assertEquals(PostgresTypes.text(), name.getType());
			assertNull(name.getSequence());
			assertNull(name.getDefaultValue());
			assertNull(name.getOutgoingForeignKey());
		}
	}

	@Test
	public void testCreatingTableWithDefaultValues() throws SQLException {
		try (Connection connection = database.createConnection()) {
			Catalog catalog = new Catalog(database.getCatalogName());
			Table users = new Table("users")
					.addColumn(new Column("id", PostgresTypes.bigint(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
					.addColumn(new Column("active", PostgresTypes.bool(), "true", Hint.NOT_NULL));

			catalog.addTable(users);

			TableCreator tableCreator = new TableCreator(database.getConfig());
			tableCreator.create(connection, Lists.newArrayList(users));
		}

		try (Connection connection = database.createConnection()) {
			Catalog catalog = CatalogLoader.load(connection, database.getCatalogName());
			Table users = catalog.getTable("users");

			Column id = users.getColumn("id");
			assertEquals(Sets.newHashSet(Hint.PRIMARY_KEY, Hint.NOT_NULL, Hint.AUTO_INCREMENT), id.getHints());
			assertEquals(PostgresTypes.bigint(), id.getType());
			assertEquals("users_id_seq", id.getSequence().getName());
			assertNull(id.getDefaultValue());
			assertNull(id.getOutgoingForeignKey());

			Column name = users.getColumn("active");
			assertEquals(Sets.newHashSet(Hint.NOT_NULL), name.getHints());
			assertEquals(PostgresTypes.bool(), name.getType());
			assertNull(name.getSequence());
			assertEquals("true", name.getDefaultValue());
			assertNull(name.getOutgoingForeignKey());
		}
	}

	@Test
	public void testCreatingTableWithUppercaseCharacters() throws SQLException {
		try (Connection connection = database.createConnection()) {
			Catalog catalog = new Catalog(database.getCatalogName());
			Table users = new Table("Users")
					.addColumn(new Column("Id", PostgresTypes.bigint(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
					.addColumn(new Column("Name", PostgresTypes.text(), Hint.NOT_NULL));

			catalog.addTable(users);

			TableCreator tableCreator = new TableCreator(database.getConfig());
			tableCreator.create(connection, Lists.newArrayList(users));
		}

		try (Connection connection = database.createConnection()) {
			Catalog catalog = CatalogLoader.load(connection, database.getCatalogName());
			Table users = catalog.getTable("Users");

			Column id = users.getColumn("Id");
			assertEquals(Sets.newHashSet(Hint.PRIMARY_KEY, Hint.NOT_NULL, Hint.AUTO_INCREMENT), id.getHints());
			assertEquals(PostgresTypes.bigint(), id.getType());
			assertEquals("Users_Id_seq", id.getSequence().getName());
			assertNull(id.getDefaultValue());
			assertNull(id.getOutgoingForeignKey());

			Column name = users.getColumn("Name");
			assertEquals(Sets.newHashSet(Hint.NOT_NULL), name.getHints());
			assertEquals(PostgresTypes.text(), name.getType());
			assertNull(name.getSequence());
			assertNull(name.getDefaultValue());
			assertNull(name.getOutgoingForeignKey());
		}
	}

	@Test
	public void testCreatingTableWithIndices() throws SQLException {
		try (Connection connection = database.createConnection()) {
			Catalog catalog = new Catalog(database.getCatalogName());
			Table users = new Table("users")
					.addColumn(new Column("id", PostgresTypes.bigint(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
					.addColumn(new Column("email", PostgresTypes.text(), Hint.NOT_NULL))
					.addIndex(new Index("email_idx", Lists.newArrayList("email"), true));

			catalog.addTable(users);

			TableCreator tableCreator = new TableCreator(database.getConfig());
			tableCreator.create(connection, Lists.newArrayList(users));
		}

		try (Connection connection = database.createConnection()) {
			Catalog catalog = CatalogLoader.load(connection, database.getCatalogName());
			Table users = catalog.getTable("users");

			Column id = users.getColumn("id");
			assertEquals(Sets.newHashSet(Hint.PRIMARY_KEY, Hint.NOT_NULL, Hint.AUTO_INCREMENT), id.getHints());
			assertEquals(PostgresTypes.bigint(), id.getType());
			assertEquals("users_id_seq", id.getSequence().getName());
			assertNull(id.getDefaultValue());
			assertNull(id.getOutgoingForeignKey());

			Column email = users.getColumn("email");
			assertEquals(Sets.newHashSet(Hint.NOT_NULL), email.getHints());
			assertEquals(PostgresTypes.text(), email.getType());
			assertNull(email.getSequence());
			assertNull(email.getDefaultValue());
			assertNull(email.getOutgoingForeignKey());

			Index index = users.getIndex("email");
			assertEquals("email_idx", index.getIndexName());
			assertEquals(Lists.newArrayList("email"), index.getColumns());
			assertTrue(index.isUnique());
		}
	}

	@Test
	public void testCreatingTableWithForeignKeys() throws SQLException {
		try (Connection connection = database.createConnection()) {
			Catalog catalog = new Catalog(database.getCatalogName());
			Table users = new Table("users")
					.addColumn(new Column("id", PostgresTypes.bigint(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
					.addColumn(new Column("name", PostgresTypes.text(), Hint.NOT_NULL));

			Table messages = new Table("messages")
					.addColumn(new Column("id", PostgresTypes.bigint(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
					.addColumn(new Column("author_id", PostgresTypes.bigint(), Hint.NOT_NULL))
					.addColumn(new Column("content", PostgresTypes.text(), Hint.NOT_NULL));

			messages.addForeignKey("author_id")
					.referencing(users, "id");

			catalog.addTable(users);
			catalog.addTable(messages);

			TableCreator tableCreator = new TableCreator(database.getConfig());
			tableCreator.create(connection, Lists.newArrayList(users, messages));
		}

		try (Connection connection = database.createConnection()) {
			Catalog catalog = CatalogLoader.load(connection, database.getCatalogName());
			Table messages = catalog.getTable("messages");

			assertEquals(1, messages.getForeignKeys().size());
			ForeignKey foreignKey = messages.getForeignKeys().get(0);

			assertEquals("users", foreignKey.getReferredTableName());
			assertEquals(Lists.newArrayList("id"), foreignKey.getReferredColumns());

			assertEquals("messages", foreignKey.getReferencingTableName());
			assertEquals(Lists.newArrayList("author_id"), foreignKey.getReferencingColumns());
		}
	}

}
