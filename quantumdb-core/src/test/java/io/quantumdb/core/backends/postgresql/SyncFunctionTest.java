package io.quantumdb.core.backends.postgresql;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.bigint;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.*;
import static io.quantumdb.core.schema.operations.SchemaOperations.alterColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropColumn;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.postgresql.migrator.NullRecords;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Changelog;
import org.junit.Test;
import org.mockito.Mockito;

public class SyncFunctionTest {

	@Test
	public void testSimpleDataMapping() {
		Table original = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table ghost = new Table("users2")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("email", varchar(255)));

		Catalog catalog = new Catalog("public");
		catalog.addTable(original);
		catalog.addTable(ghost);

		Changelog changelog = new Changelog();
		changelog.addChangeSet("Michael de Jong", SchemaOperations.addColumn("users", "email", varchar(255)));
		RefLog refLog = new RefLog();

		ColumnRef usersId = new ColumnRef("id");
		ColumnRef usersName = new ColumnRef("name");
		TableRef source = refLog.addTable(original.getName(), original.getName(), changelog.getRoot(),
				Lists.newArrayList(usersId, usersName));

		ColumnRef users2Id = new ColumnRef("id", Sets.newHashSet(usersId));
		ColumnRef users2Name = new ColumnRef("name", Sets.newHashSet(usersName));
		TableRef target = refLog.addTable(ghost.getName(), ghost.getName(), changelog.getLastAdded(),
				Lists.newArrayList(users2Id, users2Name));

		refLog.addSync("some-name", "some-function", ImmutableMap.of(usersId, users2Id, usersName, users2Name));

		NullRecords nullRecords = Mockito.mock(NullRecords.class);
		SyncFunction syncFunction = new SyncFunction(refLog, source, target, catalog, nullRecords);
		syncFunction.setColumnsToMigrate(list("id", "name", "email"));

		assertThat(syncFunction.getInsertExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateIdentities(), is(ImmutableMap.of("\"id\"", "OLD.\"id\"")));
	}

	@Test
	public void testDataMappingWithColumnRename() {
		Table original = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table ghost = new Table("users2")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("full_name", varchar(255), NOT_NULL));

		Catalog catalog = new Catalog("public");
		catalog.addTable(original);
		catalog.addTable(ghost);

		Changelog changelog = new Changelog();
		changelog.addChangeSet("Michael de Jong", alterColumn("users", "name").rename("full_name"));
		RefLog refLog = new RefLog();

		ColumnRef usersId = new ColumnRef("id");
		ColumnRef usersName = new ColumnRef("name");
		TableRef source = refLog.addTable(original.getName(), original.getName(), changelog.getRoot(),
				Lists.newArrayList(usersId, usersName));

		ColumnRef users2Id = new ColumnRef("id", Sets.newHashSet(usersId));
		ColumnRef users2Name = new ColumnRef("full_name", Sets.newHashSet(usersName));
		TableRef target = refLog.addTable(original.getName(), ghost.getName(), changelog.getLastAdded(),
				Lists.newArrayList(users2Id, users2Name));

		refLog.addSync("some-name", "some-function", ImmutableMap.of(usersId, users2Id, usersName, users2Name));

		NullRecords nullRecords = Mockito.mock(NullRecords.class);
		SyncFunction syncFunction = new SyncFunction(refLog, source, target, catalog, nullRecords);
		syncFunction.setColumnsToMigrate(list("id", "full_name"));

		assertThat(syncFunction.getInsertExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"full_name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"full_name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateIdentities(), is(ImmutableMap.of("\"id\"", "OLD.\"id\"")));
	}

	@Test
	public void testDataMappingWithColumnIdentityRename() {
		Table original = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table ghost = new Table("users2")
				.addColumn(new Column("user_id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Catalog catalog = new Catalog("public");
		catalog.addTable(original);
		catalog.addTable(ghost);

		Changelog changelog = new Changelog();
		changelog.addChangeSet("Michael de Jong", alterColumn("users", "id").rename("user_id"));
		RefLog refLog = new RefLog();

		ColumnRef usersId = new ColumnRef("id");
		ColumnRef usersName = new ColumnRef("name");
		TableRef source = refLog.addTable(original.getName(), original.getName(), changelog.getRoot(),
				Lists.newArrayList(usersId, usersName));

		ColumnRef users2Id = new ColumnRef("user_id", Sets.newHashSet(usersId));
		ColumnRef users2Name = new ColumnRef("name", Sets.newHashSet(usersName));
		TableRef target = refLog.addTable(original.getName(), ghost.getName(), changelog.getLastAdded(),
				Lists.newArrayList(users2Id, users2Name));

		refLog.addSync("some-name", "some-function", ImmutableMap.of(usersId, users2Id, usersName, users2Name));

		NullRecords nullRecords = Mockito.mock(NullRecords.class);
		SyncFunction syncFunction = new SyncFunction(refLog, source, target, catalog, nullRecords);
		syncFunction.setColumnsToMigrate(list("user_id", "name"));

		assertThat(syncFunction.getInsertExpressions(), is(ImmutableMap.of("\"user_id\"", "NEW.\"id\"", "\"name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateExpressions(), is(ImmutableMap.of("\"user_id\"", "NEW.\"id\"", "\"name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateIdentities(), is(ImmutableMap.of("\"user_id\"", "OLD.\"id\"")));
	}

	@Test
	public void testDataMappingWhereColumnIsMadeNonNullable() {
		Table original = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255)));

		Table ghost = new Table("users2")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Catalog catalog = new Catalog("public");
		catalog.addTable(original);
		catalog.addTable(ghost);

		Changelog changelog = new Changelog();
		changelog.addChangeSet("Michael de Jong", alterColumn("users", "name").addHint(NOT_NULL));
		RefLog refLog = new RefLog();

		ColumnRef usersId = new ColumnRef("id");
		ColumnRef usersName = new ColumnRef("name");
		TableRef source = refLog.addTable(original.getName(), original.getName(), changelog.getRoot(),
				Lists.newArrayList(usersId, usersName));

		ColumnRef users2Id = new ColumnRef("id", Sets.newHashSet(usersId));
		ColumnRef users2Name = new ColumnRef("name", Sets.newHashSet(usersName));
		TableRef target = refLog.addTable(original.getName(), ghost.getName(), changelog.getLastAdded(),
				Lists.newArrayList(users2Id, users2Name));

		NullRecords nullRecords = Mockito.mock(NullRecords.class);
		SyncFunction syncFunction = new SyncFunction(refLog, source, target, catalog, nullRecords);
		syncFunction.setColumnsToMigrate(list("id", "name"));

		assertThat(syncFunction.getInsertExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateIdentities(), is(ImmutableMap.of("\"id\"", "OLD.\"id\"")));
	}

	@Test
	public void testDataMappingWithNonNullableForeignKey() {
		Table original = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("other_id", bigint(), NOT_NULL))
				.addColumn(new Column("name", varchar(255)));

		Table other = new Table("other")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255)));

		Table ghost = new Table("users2")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("other_id", bigint(), NOT_NULL))
				.addColumn(new Column("full_name", varchar(255), NOT_NULL));

		original.addForeignKey("other_id").referencing(other, "id");
		ghost.addForeignKey("other_id").referencing(other, "id");

		Catalog catalog = new Catalog("public");
		catalog.addTable(original);
		catalog.addTable(other);
		catalog.addTable(ghost);

		Changelog changelog = new Changelog();
		changelog.addChangeSet("Michael de Jong", dropColumn("users", "other_id"));
		changelog.addChangeSet("Michael de Jong", alterColumn("users", "name").rename("full_name").addHint(NOT_NULL));
		RefLog refLog = new RefLog();

		ColumnRef usersId = new ColumnRef("id");
		ColumnRef usersOtherId = new ColumnRef("other_id");
		ColumnRef usersName = new ColumnRef("name");
		TableRef source = refLog.addTable(original.getName(), original.getName(), changelog.getRoot(),
				Lists.newArrayList(usersId, usersOtherId, usersName));

		ColumnRef users2Id = new ColumnRef("id", Sets.newHashSet(usersId));
		ColumnRef users2OtherId = new ColumnRef("other_id", Sets.newHashSet(usersOtherId));
		ColumnRef users2Name = new ColumnRef("full_name", Sets.newHashSet(usersName));
		TableRef target = refLog.addTable(original.getName(), ghost.getName(), changelog.getLastAdded(),
				Lists.newArrayList(users2Id, users2OtherId, users2Name));

		NullRecords nullRecords = Mockito.mock(NullRecords.class);
		Mockito.when(nullRecords.getIdentity(other)).thenReturn(new Identity("id", 0));

		SyncFunction syncFunction = new SyncFunction(refLog, source, target, catalog, nullRecords);
		syncFunction.setColumnsToMigrate(list("id", "full_name"));

		assertThat(syncFunction.getInsertExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"other_id\"", "0", "\"full_name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"other_id\"", "0", "\"full_name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateIdentities(), is(ImmutableMap.of("\"id\"", "OLD.\"id\"")));
	}

	private Set<String> list(String... inputs) {
		return Sets.newLinkedHashSet(Lists.newArrayList(inputs));
	}

}
