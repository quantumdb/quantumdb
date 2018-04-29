package io.quantumdb.cli.commands;

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
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Init extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("init", "Initializes and prepares the database for use with QuantumDB.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			if (!arguments.isEmpty()) {
				String hosts = getArgument(arguments, "host", String.class, () -> "localhost:5432");
				String catalogName = getArgument(arguments, "database", String.class);
				String user = getArgument(arguments, "username", String.class);
				String pass = getArgument(arguments, "password", String.class, null);

				config.setUrl("jdbc:postgresql://" + hosts + "/" + catalogName + "?targetServerType=master\"");
				config.setCatalog(catalogName);
				config.setUser(user);
				config.setPassword(pass);
				config.setDriver("org.postgresql.Driver");
			}

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
			if (state.getRefLog().getVersions().isEmpty()) {
				state.getRefLog().bootstrap(catalog, changelog.getRoot());
			}

			writer.indent(-1);
			writer.write("Persisting current state to database...");
			persistChanges(backend, state);

			writeDatabaseState(writer, state.getRefLog(), state.getChangelog());

			config.persist();
		}
		catch (IOException | CliException e) {
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

	private String getDatabaseVendor(Backend backend) {
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
