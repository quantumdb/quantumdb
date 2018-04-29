package io.quantumdb.cli.commands;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.versioning.QuantumTables;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Nuke extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("nuke", "Drops everything in the database.");
	}

	public void perform(CliWriter writer, List<String> arguments) throws IOException {
		Config config = Config.load();

		Backend backend = config.getBackend();

		try {
			try (Connection connection = backend.connect()) {
				QuantumTables.dropEverything(connection, "public");
			}
			writer.write("Successfully dropped everything!", Context.SUCCESS);
		}
		catch (SQLException e) {
			writer.write(e.getMessage(), Context.FAILURE);
		}

	}

}
