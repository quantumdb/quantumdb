package io.quantumdb.core.backends.postgresql;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.inject.name.Named;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.postgresql.migrator.TableCreator;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.ChangelogBackend;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.TableNameMappingBackend;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresqlBackend implements Backend {

	private final ChangelogBackend changelogBackend;
	private final TableNameMappingBackend tableNameMappingBackend;

	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPass;
	private final String jdbcCatalog;

	@Inject
	PostgresqlBackend(ChangelogBackend changelogBackend, TableNameMappingBackend tableMappings,
			@Named("javax.persistence.jdbc.url") String jdbcUrl,
			@Named("javax.persistence.jdbc.user") String jdbcUser,
			@Named("javax.persistence.jdbc.password") String jdbcPass,
			@Named("javax.persistence.jdbc.catalog") String jdbcCatalog) {

		this.changelogBackend = changelogBackend;
		this.tableNameMappingBackend = tableMappings;

		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPass = jdbcPass;
		this.jdbcCatalog = jdbcCatalog;
	}

	@Override
	public State loadState() throws SQLException {
		log.trace("Loading state from database...");
		try (Connection connection = connect()) {
			Changelog changelog = changelogBackend.load(this);
			Catalog catalog = new CatalogLoader(connection).load(jdbcCatalog);
			TableMapping tableMapping = tableNameMappingBackend.load(this, changelog);

			return new State(catalog, tableMapping, changelog);
		}
	}

	@Override
	public void persistState(State state) throws SQLException {
		log.info("Persisting state to database...");

		changelogBackend.persist(this, state.getChangelog());
		tableNameMappingBackend.persist(this, state.getTableMapping());
	}

	@Override
	public PostgresqlMigrator getMigrator() {
		return new PostgresqlMigrator(this);
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
	public TableCreator getTableCreator() {
		return new TableCreator();
	}

	@Override
	public Connection connect() throws SQLException {
		return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
	}

}
