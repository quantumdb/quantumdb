package io.quantumdb.cli.commands;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

			String changeSetId = arguments.remove(0);
			Version version = changelog.getRoot();
			while (!version.getChangeSet().getId().equals(changeSetId)) {
				version = version.getChild();
				if (version == null) {
					throw new IllegalArgumentException("Please specify a valid Changeset ID to drop!");
				}
			}
			version = version.getChangeSet().getVersion();

			String outputFile = null;
			boolean printDryRun = false;
			boolean isDryRun = getArgument(arguments, "dry-run", Boolean.class, () -> false);
			if (isDryRun) {
				outputFile = getArgument(arguments, "output-file", String.class, () -> null);
				if (outputFile == null) {
					Path path = Files.createTempFile("drop", ".sql");
					outputFile = path.toFile().getAbsolutePath();
					printDryRun = true;
				}
				else if (new File(outputFile).exists()) {
					new File(outputFile).delete();
				}

				config.enableDryRun(outputFile);
			}

			if (!isDryRun) {
				writer.write("Checking how many clients are still connected to: " + version.getChangeSet().getId());
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
			}

			writer.write("Dropping database schema version: " + version.getChangeSet().getId() + "...");

			backend.getMigrator().drop(state, version);

			if (isDryRun) {
				if (printDryRun) {
					System.out.println(String.join("\n", Files.readAllLines(new File(outputFile).toPath())));
				}
			}
			else {
				state = loadState(backend);
				writeDatabaseState(writer, state.getRefLog(), state.getChangelog());
			}
		}
		catch (MigrationException | IOException | CliException | SQLException e) {
			log.error(e.getMessage(), e);
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

}
