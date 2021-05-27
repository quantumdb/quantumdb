package io.quantumdb.core.planner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import io.quantumdb.core.backends.Config;
import io.quantumdb.core.utils.OutputFile;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryUtils {

	public static String quoted(String input) {
		if (input == null) {
			return null;
		}
		else if (input.length() >= 2 && input.charAt(0) == '\"' && input.charAt(input.length() - 1) == '\"') {
			return input;
		}
		return "\"" + input + "\"";
	}

	public static void execute(Connection connection, Config config, String query) throws SQLException {
		if (config.isDryRun()) {
			OutputFile.append(config.getOutputFile(), query);
		}
		else {
			try (Statement statement = connection.createStatement()) {
				log.debug("Executing: " + query);
				statement.execute(query);
			}
		}
	}

}
