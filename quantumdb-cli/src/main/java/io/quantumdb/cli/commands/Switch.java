package io.quantumdb.cli.commands;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.planner.Aliaser;
import io.quantumdb.core.planner.PostgresqlBackend;

public class Switch extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("switch", "Switches all aliases to a specified version.");
	}

	@Override
	public void perform(CliWriter writer, List<String> arguments) throws IOException {
		Config config = Config.load();
		Backend backend = config.getBackend();

		String versionId = arguments.remove(0);
		try {
			new Aliaser((PostgresqlBackend) backend).switchTo(versionId);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
