package io.quantumdb.cli.commands;

import java.io.IOException;
import java.util.List;

import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Status extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("status", "Display currently available versions of the database schema.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			Backend backend = config.getBackend();

			State state = loadState(backend);
			TableMapping tableMapping = state.getTableMapping();
			writeDatabaseState(writer, tableMapping);
		}
		catch (IOException | CliException e) {
			log.error(e.getMessage(), e);
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

}
