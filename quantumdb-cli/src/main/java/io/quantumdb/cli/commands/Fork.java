package io.quantumdb.cli.commands;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
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

			Version to = changelog.getVersion(arguments.remove(0));
			Version from = getOriginVersion(arguments, state, changelog);

			writer.write("Forking database from: " + from.getId() + " to: " + to.getId() + "...");

			backend.getMigrator().migrate(state, from, to);

			state = loadState(backend);
			writeDatabaseState(writer, state.getRefLog());
		}
		catch (MigrationException | IOException | CliException e) {
			log.error(e.getMessage(), e);
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

	private Version getOriginVersion(List<String> arguments, State state, Changelog changelog) throws CliException {
		if (arguments.isEmpty()) {
			List<Version> versions = Lists.newArrayList(state.getRefLog().getVersions());
			if (versions.size() == 1) {
				return versions.get(0);
			}
			throw new CliException("You must specify a version to fork from!");
		}
		return changelog.getVersion(arguments.remove(0));
	}

}
