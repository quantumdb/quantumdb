package io.quantumdb.cli.commands;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Drop extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("drop", "Drops the specified version of the database schema.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			Backend backend = config.getBackend();
			writer.write("Scanning database...");

			State state = loadState(backend);
			Changelog changelog = state.getChangelog();

			Version version = changelog.getVersion(arguments.remove(0));

			writer.write("Checking how many clients are still connected to: " + version.getId());
			int count = backend.countClientsConnectedToVersion(version);

			if (count > 0) {
				writer.indent(1)
						.write("There are still " + count + " clients using this version.", Context.FAILURE)
						.indent(-1);
				return;
			}
			else {
				writer.indent(1)
						.write("0 clients are using this version of the database schema.")
						.indent(-1);
			}

			writer.write("Dropping database schema version: " + version.getId() + "...");

			backend.getMigrator().drop(state, version);

			state = loadState(backend);
			writeDatabaseState(writer, state.getRefLog(), state.getChangelog());
		}
		catch (MigrationException | IOException | CliException | SQLException e) {
			log.error(e.getMessage(), e);
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

}
