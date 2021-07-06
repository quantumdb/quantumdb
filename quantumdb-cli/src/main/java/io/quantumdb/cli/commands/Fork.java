package io.quantumdb.cli.commands;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.migration.Migrator;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Fork extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("fork", "Forks an existing database schema, and applies a set of operations to the fork.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			Backend backend = config.getBackend();
			writer.write("Scanning database...");

			State state = loadState(backend);
			Changelog changelog = state.getChangelog();

			Version from = getOriginVersion(arguments, state, changelog);

			String toChangeSet = arguments.remove(0);
			Version version = changelog.getRoot();
			while (!version.getChangeSet().getId().equals(toChangeSet)) {
				version = version.getChild();
				if (version == null) {
					throw new IllegalArgumentException("Please specify a valid Changeset ID to fork to!");
				}
			}
			Version to = version.getChangeSet().getVersion();

			String outputFile = null;
			boolean printDryRun = false;
			boolean isDryRun = getArgument(arguments, "dry-run", Boolean.class, () -> false);
			if (isDryRun) {
				outputFile = getArgument(arguments, "output-file", String.class, () -> null);
				if (outputFile == null) {
					Path path = Files.createTempFile("fork", ".sql");
					outputFile = path.toFile().getAbsolutePath();
					printDryRun = true;
				}
				else if (new File(outputFile).exists()) {
					new File(outputFile).delete();
				}

				config.enableDryRun(outputFile);
			}

			writer.write("Forking database from: " + from.getId() + " to: " + to.getId() + "...");

			Migrator migrator = new Migrator(backend);
			migrator.migrate(from.getId(), to.getId());

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

	private Version getOriginVersion(List<String> arguments, State state, Changelog changelog) {
		String changesetId = getArgument(arguments, "from", String.class, () -> {
			List<Version> versions = Lists.newArrayList(state.getRefLog().getVersions());
			Version version = changelog.getRoot();
			Version lastVersion = version;

			version = version.getChild();
			while (version != null) {
				if (versions.contains(version)) {
					lastVersion = version;
				}
				version = version.getChild();
			}

			return lastVersion.getChangeSet().getId();
		});

		Version version = changelog.getRoot();
		while (!version.getChangeSet().getId().equals(changesetId)) {
			version = version.getChild();
		}

		return version.getChangeSet().getVersion();
	}

}
