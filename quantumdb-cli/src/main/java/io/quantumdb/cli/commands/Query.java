package io.quantumdb.cli.commands;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Config;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Query extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("query", "Execute a query on a specific version of the database schema.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			String versionId = arguments.remove(0);
			String query = arguments.remove(0);

			Class.forName("io.quantumdb.driver.Driver");
			String url = createUrl(config.getUrl(), config.getCatalog(), versionId);
			try (Connection connection = DriverManager.getConnection(url, config.getUser(), config.getPassword())) {
				Statement statement = connection.createStatement();

				if (query.toLowerCase().trim().startsWith("select")) {
					ResultSet resultSet = statement.executeQuery(query);

					boolean results = false;
					while (resultSet.next()) {
						results = true;
						System.out.println("{");
						for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
							String columnName = resultSet.getMetaData().getColumnName(i);
							Object value = resultSet.getObject(columnName);
							String terminus = (i < resultSet.getMetaData().getColumnCount()) ? "," : "";
							System.out.println("\t" + columnName + ": " + value + terminus);
						}
						System.out.println("}");
					}


					if (!results) {
						writer.write("No results found!");
					}
				}
				else {
					int affected = statement.executeUpdate(query);
					writer.write("Affected rows: " + affected);
				}
			}
		}
		catch (ClassNotFoundException | SQLException | IOException e) {
			log.error(e.getMessage(), e);
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

	private String createUrl(String url, String catalog, String versionId) {
		return url.replace("jdbc:", "jdbc:quantumdb:") + "/" + catalog + "?version=" + versionId;
	}

}
