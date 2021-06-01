package io.quantumdb.core.planner;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import com.google.common.collect.Sets;
import io.quantumdb.core.backends.PostgresqlDatabase;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.PostgresTypes;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import org.junit.Rule;
import org.junit.Test;

public class SyncFunctionTest {

	@Rule
	public final PostgresqlDatabase database = new PostgresqlDatabase();

	@Test
	public void createSimpleSyncFunction() {
		RefLog refLog = new RefLog();
		Version v1 = new Version("v1", null);
		Version v2 = new Version("v2", v1);

		TableRef t1 = refLog.addTable("users", "table_a", v1,
				new ColumnRef("id"),
				new ColumnRef("name"));

		TableRef t2 = refLog.addTable("users", "table_b", v2,
				new ColumnRef("id", t1.getColumn("id")),
				new ColumnRef("name", t1.getColumn("name")));

		Catalog catalog = new Catalog(database.getCatalogName());

		catalog.addTable(new Table("table_a")
				.addColumn(new Column("id", PostgresTypes.bigint(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
				.addColumn(new Column("name", PostgresTypes.bigint(), Hint.NOT_NULL)));

		catalog.addTable(new Table("table_b")
				.addColumn(new Column("id", PostgresTypes.bigint(), Hint.PRIMARY_KEY, Hint.AUTO_INCREMENT))
				.addColumn(new Column("name", PostgresTypes.bigint(), Hint.NOT_NULL)));

		NullRecords nullRecords = new NullRecords(database.getConfig());

		Map<ColumnRef, ColumnRef> columnMapping = refLog.getColumnMapping(t1, t2);
		SyncFunction function = new SyncFunction(refLog, t1, t2, columnMapping, catalog, nullRecords,
				"migrate_data", "migration_trigger");

		function.setColumnsToMigrate(Sets.newHashSet("id", "name"));

		String createFunctionStatement = function.createFunctionStatement().toString();
		String createTriggerStatement = function.createTriggerStatement().toString();

		assertEquals("CREATE OR REPLACE FUNCTION \"migrate_data\"() RETURNS TRIGGER AS $$ BEGIN IF TG_OP = 'INSERT' THEN INSERT INTO \"table_b\" (\"name\", \"id\") VALUES (NEW.\"name\", NEW.\"id\"); ELSIF TG_OP = 'UPDATE' THEN LOOP UPDATE \"table_b\" SET \"id\" = NEW.\"id\", \"name\" = NEW.\"name\" WHERE \"id\" = OLD.\"id\"; IF found THEN EXIT; END IF; BEGIN INSERT INTO \"table_b\" (\"name\", \"id\") VALUES (NEW.\"name\", NEW.\"id\"); EXIT; EXCEPTION WHEN unique_violation THEN END; END LOOP; ELSIF TG_OP = 'DELETE' THEN DELETE FROM \"table_b\" WHERE \"id\" = OLD.\"id\"; END IF; RETURN NEW; END; $$ LANGUAGE 'plpgsql';", createFunctionStatement);
		assertEquals("CREATE TRIGGER \"migration_trigger\" AFTER INSERT OR UPDATE OR DELETE ON \"table_a\" FOR EACH ROW WHEN (pg_trigger_depth() = 0) EXECUTE PROCEDURE \"migrate_data\"();", createTriggerStatement);
	}

}
