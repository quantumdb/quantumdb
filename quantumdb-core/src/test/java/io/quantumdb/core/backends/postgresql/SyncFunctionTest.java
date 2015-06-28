package io.quantumdb.core.backends.postgresql;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.bigint;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.postgresql.migrator.NullRecords;
import io.quantumdb.core.migration.utils.DataMapping;
import io.quantumdb.core.migration.utils.DataMapping.Transformation;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
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

		DataMapping dataMapping = new DataMapping(original, ghost);
		dataMapping.setColumnMapping("id", "id", Transformation.createNop());
		dataMapping.setColumnMapping("name", "name", Transformation.createNop());

		SyncFunction syncFunction = new SyncFunction(dataMapping, new NullRecords());
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

		DataMapping dataMapping = new DataMapping(original, ghost);
		dataMapping.setColumnMapping("id", "id", Transformation.createNop());
		dataMapping.setColumnMapping("name", "full_name", Transformation.createNop());

		SyncFunction syncFunction = new SyncFunction(dataMapping, new NullRecords());
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

		DataMapping dataMapping = new DataMapping(original, ghost);
		dataMapping.setColumnMapping("id", "user_id", Transformation.createNop());
		dataMapping.setColumnMapping("name", "name", Transformation.createNop());

		SyncFunction syncFunction = new SyncFunction(dataMapping, new NullRecords());
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

		DataMapping dataMapping = new DataMapping(original, ghost);
		dataMapping.setColumnMapping("id", "id", Transformation.createNop());
		dataMapping.setColumnMapping("name", "name", Transformation.createNop());

		SyncFunction syncFunction = new SyncFunction(dataMapping, new NullRecords());
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

		DataMapping dataMapping = new DataMapping(original, ghost);
		dataMapping.setColumnMapping("id", "id", Transformation.createNop());
		dataMapping.setColumnMapping("other_id", "other_id", Transformation.createNop());
		dataMapping.setColumnMapping("name", "full_name", Transformation.createNop());

		NullRecords nullRecords = Mockito.mock(NullRecords.class);
		Mockito.when(nullRecords.getIdentity(other)).thenReturn(new Identity("id", 0));

		SyncFunction syncFunction = new SyncFunction(dataMapping, nullRecords);
		syncFunction.setColumnsToMigrate(list("id", "full_name"));

		assertThat(syncFunction.getInsertExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"other_id\"", "0", "\"full_name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateExpressions(), is(ImmutableMap.of("\"id\"", "NEW.\"id\"", "\"other_id\"", "0", "\"full_name\"", "NEW.\"name\"")));
		assertThat(syncFunction.getUpdateIdentities(), is(ImmutableMap.of("\"id\"", "OLD.\"id\"")));
	}

	private Set<String> list(String... inputs) {
		return Sets.newLinkedHashSet(Lists.newArrayList(inputs));
	}

}
