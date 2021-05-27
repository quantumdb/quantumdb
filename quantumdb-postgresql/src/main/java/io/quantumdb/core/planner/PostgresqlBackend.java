package io.quantumdb.core.planner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.quantumdb.core.backends.Config;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.versioning.Backend;
import io.quantumdb.core.versioning.QuantumTables;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresqlBackend implements io.quantumdb.core.backends.Backend {

	private final Backend backend;
	private final Config config;

	public PostgresqlBackend(Config config) {
		this.config = config;
		this.backend = new Backend();
	}

	@Override
	public State loadState() throws SQLException {
		log.trace("Loading state from database...");
		try (Connection connection = connect()) {
			QuantumTables.prepare(connection);
			Catalog catalog = CatalogLoader.load(connection, config.getCatalog());
			return backend.load(connection, catalog);
		}
	}

	@Override
	public void persistState(State state) throws SQLException {
		if (config.isDryRun()) {
			return;
		}

		log.info("Persisting state to database...");

		try (Connection connection = connect()) {
			connection.setAutoCommit(false);
			backend.persist(connection, state);
			connection.commit();
		}
	}

	@Override
	public PostgresqlMigrator getMigrator() {
		return new PostgresqlMigrator(this, config);
	}

	@Override
	public boolean isJdbcUrlSupported(String jdbcUrl) {
		return jdbcUrl.startsWith("jdbc:postgresql:");
	}

	@Override
	public int countClientsConnectedToVersion(Version version) throws SQLException {
		try (Connection connection = connect()) {
			String query = "SELECT COUNT(*) AS cnt FROM pg_stat_activity WHERE application_name LIKE ?;";
			PreparedStatement statement = connection.prepareStatement(query);
			statement.setString(1, " - " + version.getId());

			ResultSet resultSet = statement.executeQuery();
			if (resultSet.next()) {
				return resultSet.getInt("cnt");
			}
			throw new SQLException("Query produced 0 rows!");
		}
	}

	@Override
	@SneakyThrows(ClassNotFoundException.class)
	public Connection connect() throws SQLException {
		Class.forName(config.getDriver());
		Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
		try (Statement statement = connection.createStatement()) {
			statement.execute("SET SCHEMA 'public';");
		}
		return connection;
	}

}
