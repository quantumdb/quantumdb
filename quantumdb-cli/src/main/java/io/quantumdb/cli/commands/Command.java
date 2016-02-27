package io.quantumdb.cli.commands;

import java.sql.SQLException;
import java.util.List;

import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.ChangeSet;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Command {

	@Data
	public static class Identifier implements Comparable<Identifier> {
		private final String command;
		private final String description;

		@Override
		public int compareTo(Identifier o) {
			return command.compareTo(o.command);
		}
	}

	public abstract Identifier getIdentifier();

	public abstract void perform(CliWriter writer, List<String> arguments);

	void persistChanges(Backend backend, State state) throws CliException {
		try {
			backend.persistState(state);
		}
		catch (SQLException e) {
			log.error(e.getMessage(), e);
			throw new CliException("Could not initialize the database.", e);
		}
	}

	State loadState(Backend backend) throws CliException {
		try {
			return backend.loadState();
		}
		catch (SQLException e) {
			log.error(e.getMessage(), e);
			throw new CliException("Could not connect to the database.", e);
		}
	}

	void writeDatabaseState(CliWriter writer, RefLog refLog) {
		writer.write("Database is operating at version(s):", Context.SUCCESS);
		writer.indent(1);

		for (Version version : refLog.getVersions()) {
			ChangeSet changeSet = version.getChangeSet();
			writer.write(version.getId() + ": " + changeSet.getDescription(), Context.SUCCESS);
		}

		writer.indent(-1);
	}

}
