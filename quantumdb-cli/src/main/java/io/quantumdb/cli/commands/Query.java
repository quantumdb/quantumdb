package io.quantumdb.cli.commands;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Query extends Command {

	private interface ConnectionConsumer {
		void consume(Connection connection) throws Throwable;
	}

	@Override
	public Identifier getIdentifier() {
		return new Identifier("query", "Execute a query on a specific version of the database schema.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			Version version = getVersionId(arguments, config);
			String query = arguments.stream()
					.collect(Collectors.joining(" "));

			Class.forName("io.quantumdb.driver.Driver");

			try {
				executeAndParseResultSet(writer, config, version, query);
			}
			catch (Throwable e) {
				executeUpdate(writer, config, version, query);
			}
		}
		catch (Throwable e) {
			log.error(e.getMessage(), e);
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

	private void executeUpdate(CliWriter writer, Config config, Version version, String query) throws Throwable {
		doInTransaction(config, version, connection -> {
			Statement statement = connection.createStatement();
			int affected = statement.executeUpdate(query);
			writer.write("Affected rows: " + affected);
		});
	}

	private void executeAndParseResultSet(CliWriter writer, Config config, Version version, String query) throws Throwable {
		doInTransaction(config, version, connection -> {
			Statement statement = connection.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY);
			ResultSet resultSet = statement.executeQuery(query);

			Map<String, Boolean> padLeft = Maps.newHashMap();
			List<String> columnNames = Lists.newArrayList();
			ResultSetMetaData metaData = resultSet.getMetaData();

			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				String columnName = metaData.getColumnName(i);
				Class<?> type = Class.forName(metaData.getColumnClassName(i));
				padLeft.put(columnName, Number.class.isAssignableFrom(type));
				columnNames.add(columnName);
			}

			Map<String, Integer> columnWidths = Maps.newLinkedHashMap();
			for (String columnName : columnNames) {
				columnWidths.put(columnName, columnName.length());
			}

			long results = 0;
			while (resultSet.next()) {
				results++;
				for (String columnName : columnNames) {
					Integer width = columnWidths.getOrDefault(columnName, 0);
					Object object = resultSet.getObject(columnName);
					String value = object == null ? "" : Objects.toString(object);
					int currentLength = value.length();
					if (width < currentLength) {
						columnWidths.put(columnName, currentLength);
					}
				}
			}

			if (results > 0) {
				StringBuilder builder = new StringBuilder();
				for (int i = 0; i < columnNames.size(); i++) {
					String columnName = columnNames.get(i);
					if (i > 0) {
						builder.append("|");
					}
					builder.append(" ");

					int width = columnWidths.get(columnName);
					if (padLeft.get(columnName)) {
						builder.append(Strings.padStart(columnName, width, ' '));
					}
					else {
						builder.append(Strings.padEnd(columnName, width, ' '));
					}
					builder.append(" ");
				}

				int length = builder.length();
				builder.append("\n")
						.append(Strings.repeat("-", length))
						.append("\n");

				resultSet.beforeFirst();
				while (resultSet.next()) {
					for (int i = 0; i < columnNames.size(); i++) {
						String columnName = columnNames.get(i);
						if (i > 0) {
							builder.append("|");
						}
						builder.append(" ");

						int columnWidth = columnWidths.get(columnName);
						Object object = resultSet.getObject(columnName);
						String value = object == null ? "" : Objects.toString(object);
						if (padLeft.get(columnName)) {
							builder.append(Strings.padStart(value, columnWidth, ' '));
						}
						else {
							builder.append(Strings.padEnd(value, columnWidth, ' '));
						}

						builder.append(" ");
					}
					builder.append("\n");
				}
				builder.append("(")
						.append(results)
						.append(" rows)\n");

				writer.indent(-1)
						.enableBold(false)
						.write(builder.toString())
						.enableBold(true);
			}
			else {
				writer.indent(-1)
						.enableBold(false)
						.write("--\n(0 rows)\n")
						.enableBold(true);
			}
		});
	}

	private void doInTransaction(Config config, Version version, ConnectionConsumer consumer) throws Throwable {
		String url = createUrl(config.getUrl(), config.getCatalog(), version.getId());
		try (Connection connection = DriverManager.getConnection(url, config.getUser(), config.getPassword())) {
			connection.setAutoCommit(false);
			try {
				consumer.consume(connection);
				connection.commit();
			}
			catch (Throwable e) {
				connection.rollback();
				throw e;
			}
		}
	}

	private String createUrl(String url, String catalog, String versionId) {
		return url.replace("jdbc:", "jdbc:quantumdb:") + "/" + catalog + "?version=" + versionId;
	}

	private Version getVersionId(List<String> arguments, Config config) {
		State state = loadState(config.getBackend());
		String versionId = getArgument(arguments, "version", String.class, () -> {
			List<Version> versions = Lists.newArrayList(state.getRefLog().getVersions());
			if (versions.isEmpty()) {
				versions.add(state.getChangelog().getRoot());
			}

			if (versions.size() == 1) {
				return versions.get(0).getId();
			}
			throw new CliException("You must specify a version to query!");
		});

		return Optional.ofNullable(versionId)
				.map(state.getChangelog()::getVersion)
				.orElseThrow(() -> new CliException("You must specify a (valid) version to query"));
	}

}
