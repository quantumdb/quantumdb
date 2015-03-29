package io.quantumdb.core.backends.postgresql;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.google.inject.name.Named;
import com.google.inject.persist.Transactional;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.ChangelogBackend;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.TableNameMappingBackend;
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
	@Transactional
	public State loadState() throws SQLException {
		log.trace("Loading state from database...");
		try (Connection connection = connect()) {
			Changelog changelog = changelogBackend.load();
			Catalog catalog = new CatalogLoader(connection).load(jdbcCatalog);
			TableMapping tableMapping = tableNameMappingBackend.load(changelog);

			return new State(catalog, tableMapping, changelog);
		}
	}

	@Override
	@Transactional
	public void persistState(State state) throws SQLException {
		log.info("Persisting state to database...");

		changelogBackend.persist(state.getChangelog());
		tableNameMappingBackend.persist(state.getChangelog(), state.getTableMapping());
	}

	@Override
	public PostgresqlMigrator getMigrator() {
		return new PostgresqlMigrator(this);
	}

	@Override
	public Connection connect() throws SQLException {
		return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
	}

}
