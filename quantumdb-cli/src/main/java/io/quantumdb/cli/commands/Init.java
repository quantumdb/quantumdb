package io.quantumdb.cli.commands;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.bigint;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.bool;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.text;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.timestamp;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.uuid;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.ForeignKey.Action.CASCADE;
import static io.quantumdb.core.schema.definitions.ForeignKey.Action.SET_NULL;
import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.addForeignKey;
import static io.quantumdb.core.schema.operations.SchemaOperations.alterColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.createIndex;
import static io.quantumdb.core.schema.operations.SchemaOperations.createTable;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropForeignKey;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropIndex;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropTable;
import static io.quantumdb.core.schema.operations.SchemaOperations.renameTable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Init extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("init", "Initializes and prepares the database for use with QuantumDB.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			String url = arguments.remove(0);
			String catalogName = arguments.remove(0);
			String user = arguments.remove(0);
			String pass = arguments.remove(0);

			Config config = new Config();
			config.setUrl(url);
			config.setCatalog(catalogName);
			config.setUser(user);
			config.setPassword(pass);
			config.setDriver("org.postgresql.Driver");

			Backend backend = config.getBackend();

			writer.write("Scanning database...");
			State state = loadState(backend);
			Catalog catalog = state.getCatalog();
			Changelog changelog = state.getChangelog();

			writer.indent(1);
			writer.write("Vendor: " + getDatabaseVendor(backend));
			writer.write("Found: " + catalog.getTables().size() + " tables");
			writer.write("Found: " + catalog.getForeignKeys().size() + " foreign keys");
			writer.write("Found: " + catalog.getSequences().size() + " sequences");

			// Register pre-existing tables in current version.
			TableMapping mapping = state.getTableMapping();
			Version current = changelog.getRoot();
			for (Table table : catalog.getTables()) {
				mapping.add(current, table.getName(), table.getName());
			}

			writer.indent(-1);
			writer.write("Persisting current state to database...");
			persistChanges(backend, state);

			writeDatabaseState(writer, state.getTableMapping());

			config.persist();
		}
		catch (IOException | CliException e) {
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

	private String getDatabaseVendor(Backend backend) throws CliException {
		try (Connection connection = backend.connect()) {
			DatabaseMetaData metaData = connection.getMetaData();
			return metaData.getDatabaseProductName() + " " + metaData.getDatabaseProductVersion();
		}
		catch (SQLException e) {
			log.error(e.getMessage(), e);
			throw new CliException("Could not determine database vendor.", e);
		}
	}

}
