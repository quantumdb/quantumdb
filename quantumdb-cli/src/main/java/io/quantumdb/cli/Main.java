package io.quantumdb.cli;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.quantumdb.cli.commands.Changelog;
import io.quantumdb.cli.commands.Command;
import io.quantumdb.cli.commands.Command.Identifier;
import io.quantumdb.cli.commands.Drop;
import io.quantumdb.cli.commands.Fork;
import io.quantumdb.cli.commands.Init;
import io.quantumdb.cli.commands.Nuke;
import io.quantumdb.cli.commands.Query;
import io.quantumdb.cli.commands.Status;
import io.quantumdb.cli.utils.CliWriter;

public class Main {

	public static void main(String[] args) throws IOException, SQLException {
		CliWriter writer = new CliWriter();
		List<String> arguments = normalize(args);

		LinkedHashMap<String, Command> commands = listCommands();

		if (arguments.isEmpty()) {
			printHelp(writer, commands);
		}
		else {
			String command = arguments.remove(0);
			Command delegate = commands.get(command);
			if (delegate != null) {
				delegate.perform(writer, arguments);
			}
			else {
				printHelp(writer, commands);
			}
		}
	}

	private static void printHelp(CliWriter writer, LinkedHashMap<String, Command> commands) {
		writer.write("Available commands:")
				.indent(1);

		commands.forEach((command, delegate) -> {
			Identifier identifier = delegate.getIdentifier();
			writer.write(identifier.getCommand() + ": " + identifier.getDescription());
		});
		writer.indent(-1);
	}

	private static LinkedHashMap<String, Command> listCommands() {
		List<Command> commands = Lists.newArrayList(
				new Init(),
				new Changelog(),
				new Status(),
				new Fork(),
				new Nuke(),
				new Drop(),
				new Query()
		);

		LinkedHashMap<String, Command> result = Maps.newLinkedHashMap();
		commands.forEach(command -> result.put(command.getIdentifier().getCommand(), command));
		return result;
	}

	public static List<String> normalize(String[] args) {
		return Arrays.stream(args)
				.map(String::trim)
				.filter(input -> !Strings.isNullOrEmpty(input))
				.collect(Collectors.toList());
	}

}
