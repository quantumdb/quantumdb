package io.quantumdb.core.backends.postgresql;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.quantumdb.core.backends.postgresql.migrator.TableCreator;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

@Slf4j
public class TransactionalityTest {

	private static final String SYNC_FUNCTION = "sync_function";
	private static final String SYNC_TRIGGER = "sync_trigger";
	private static final String MIGRATE_FUNCTION = "migrate_function";

	private static final int ROWS = 100;

	private static final String[] NAMES = { "Karol Haycock", "Mitsuko Schulz", "Delena Tober", "Emerald Blain",
			"Carlena Sica", "Chance Halliday", "Vanna Blea", "Noella Parham", "Lupita Villalvazo", "Lekisha Otte" };

	@Rule
	public final PostgresqlDatabase database = new PostgresqlDatabase();

	private final Random random = new Random();

	@Before
	public void setup() throws SQLException {
		Catalog catalog = new Catalog(database.getCatalogName());

		Table source = new Table("source")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		Table target = new Table("target")
				.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		catalog.addTable(source);
		catalog.addTable(target);

		Connection connection = database.getConnection();

		log.info("Creating source and target table...");
		new TableCreator().create(connection, catalog.getTables());

		log.info("Creating functions and triggers...");
		execute(connection, createSyncFunction(SYNC_FUNCTION, target.getName()));
		execute(connection, createSyncTrigger(SYNC_TRIGGER, SYNC_FUNCTION, source.getName()));
		execute(connection, createMigrationFunction(MIGRATE_FUNCTION, source.getName(), target.getName(), 10));

		log.info("Inserting random test data...");
		insertRandomData(connection, source.getName(), ROWS);
	}

	@Test
	public void crossTest() throws ExecutionException, InterruptedException, SQLException {
		log.info("Starting cross test...");
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

		Future<?> future = executor.schedule(() -> {
			try (Connection conn1 = database.createConnection()) {
				log.info("Updating all records in source table in reversed order...");
				for (int id = ROWS; id > 0; id--) {
					try (Statement statement = conn1.createStatement()) {
						statement.execute("UPDATE source SET name = 'Michael de Jong' WHERE id = " + id);
						Thread.sleep(10);
					}
					log.info("Updated id: " + id);
				}
			}
			catch (SQLException | InterruptedException e) {
				log.error("Error while updating records in source table: " + e.getMessage(), e);
			}
		}, 5, TimeUnit.SECONDS);

		log.info("Running migration of records from source to target table...");

		int lastId = 0;
		Connection connection = database.getConnection();
		while (true) {
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery("SELECT * FROM " + MIGRATE_FUNCTION + "(" + lastId + ");");
				if (resultSet.next()) {
					int result = resultSet.getInt(1);
					if (result > lastId) {
						log.info("Migrated up until id: " + lastId);
						lastId = result;
						continue;
					}
				}
				break;
			}
		}

		log.info("Awaiting termination of updater thread...");
		future.get();

		log.info("Verifying consistency between source and target table...");
		verifyConsistencyTables(connection);
	}

	private void verifyConsistencyTables(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) cnt FROM target WHERE name != 'Michael de Jong';");
			if (resultSet.next()) {
				int count = resultSet.getInt("cnt");
				Assert.assertEquals(0, count);

				log.info("All records in the target table are consistent with the source table!");
			}
			else {
				throw new IllegalStateException("ResultSet was empty, but expected one records!");
			}
		}
	}

	private String createMigrationFunction(String functionName, String source, String target, int batchSize) {
		return new QueryBuilder("CREATE FUNCTION " + functionName + "(pointer int) RETURNS text AS $$")
				.append("  DECLARE r record;")
				.append("  BEGIN")
				.append("	FOR r IN")
				.append("	  SELECT * FROM " + source + " WHERE id > pointer ORDER BY id ASC LIMIT " + batchSize)
				.append("	LOOP")
				.append("	  BEGIN")
				.append("		INSERT INTO " + target + " (id, name) VALUES (r.id, r.name);")
				.append("	  EXCEPTION WHEN unique_violation THEN END;")
				.append("	  PERFORM pg_sleep(0.1);")
				.append("	END LOOP;")
				.append("  RETURN r.id;")
				.append("END; $$ LANGUAGE 'plpgsql';")
				.toString();
	}

	private String createSyncTrigger(String triggerName, String functionName, String source) {
		return new QueryBuilder()
				.append("CREATE TRIGGER " + triggerName)
				.append("AFTER INSERT OR UPDATE OR DELETE")
				.append("ON " + source)
				.append("FOR EACH ROW")
				.append("WHEN (pg_trigger_depth() = 0)")
				.append("EXECUTE PROCEDURE " + functionName + "();")
				.toString();
	}

	private String createSyncFunction(String functionName, String target) {
		return new QueryBuilder()
				.append("CREATE FUNCTION " + functionName + "()")
				.append("RETURNS TRIGGER AS $$")
				.append("BEGIN")
				.append("   IF TG_OP = 'INSERT' THEN")
				.append("	   LOOP")
				.append("		   UPDATE " + target + " SET name = NEW.name WHERE id = NEW.id;")
				.append("		   IF found THEN EXIT; END IF;")
				.append("		   BEGIN")
				.append("			   INSERT INTO " + target + " (id, name) VALUES (NEW.id, NEW.name);")
				.append("			   EXIT;")
				.append("		   EXCEPTION WHEN unique_violation THEN")
				.append("		   END;")
				.append("	   END LOOP;")
				.append("   ELSIF TG_OP = 'UPDATE' THEN")
				.append("	   LOOP")
				.append("		   UPDATE " + target + " SET name = NEW.name WHERE id = NEW.id;")
				.append("		   IF found THEN EXIT; END IF;")
				.append("		   BEGIN")
				.append("			   INSERT INTO " + target + " (id, name) VALUES (NEW.id, NEW.name);")
				.append("			   EXIT;")
				.append("		   EXCEPTION WHEN unique_violation THEN")
				.append("		   END;")
				.append("	   END LOOP;")
				.append("   ELSIF TG_OP = 'DELETE' THEN")
				.append("	   DELETE FROM " + target + " WHERE id = OLD.id;")
				.append("   END IF;")
				.append("   RETURN NEW;")
				.append("END;")
				.append("$$ LANGUAGE 'plpgsql';")
				.toString();
	}

	private void insertRandomData(Connection connection, String table, int records) throws SQLException {
		String query = "INSERT INTO " + table + " (name) VALUES (?);";
		try {
			connection.setAutoCommit(false);
			try (PreparedStatement statement = connection.prepareStatement(query)) {
				for (int i = 0; i < records; i++) {
					statement.setString(1, pickRandomName());
					statement.addBatch();
				}
				statement.executeBatch();
			}
			connection.commit();
		}
		catch (SQLException e) {
			log.error("Error while inserting random test data: " + e.getMessage(), e);
			connection.rollback();
		}
		finally {
			connection.setAutoCommit(true);
		}
	}

	private String pickRandomName() {
		return NAMES[random.nextInt(NAMES.length)];
	}

	private void execute(Connection connection, String query) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
		}
	}

}
