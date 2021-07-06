package io.quantumdb.cli.commands;

import static io.quantumdb.core.schema.operations.SchemaOperations.cleanupTables;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.migration.Migrator;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.ChangeSet;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Cleanup extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("cleanup", "Renames all tables back to their original name.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			Backend backend = config.getBackend();
			writer.write("Scanning database...");

			State state = loadState(backend);
			Changelog changelog = state.getChangelog();

			Version last = changelog.getVersion(state.getRefLog().getVersions().toArray(new Version[0])[0].getId());
			writer.write(last.getId());
			ChangeSet changeSet = new ChangeSet("cleanup_" + RandomHasher.generateHash(), "QuantumDB");
			Operation operation = cleanupTables();
			changelog.addChangeSet(last, changeSet, operation);

			String outputFile = null;
			boolean printDryRun = false;
			boolean isDryRun = getArgument(arguments, "dry-run", Boolean.class, () -> false);
			if (isDryRun) {
				outputFile = getArgument(arguments, "output-file", String.class, () -> null);
				if (outputFile == null) {
					Path path = Files.createTempFile("cleanup", ".sql");
					outputFile = path.toFile().getAbsolutePath();
					printDryRun = true;
				}
				else if (new File(outputFile).exists()) {
					new File(outputFile).delete();
				}

				config.enableDryRun(outputFile);
			}

			writer.write("Renaming all tables back to their original name.");

			Migrator migrator = new Migrator(backend);
			migrator.migrate(last.getId(), changelog.getLastAdded().getId());

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
		catch (MigrationException | IOException | CliException e) {
			log.error(e.getMessage(), e);
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

}
