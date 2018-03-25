package io.quantumdb.core.planner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.Data;

@Data
public class Aliaser {

	private final PostgresqlBackend backend;

	public void switchTo(String versionId) throws SQLException {
		State state = backend.loadState();
		RefLog refLog = state.getRefLog();
		Changelog changelog = state.getChangelog();

		Version version = null;
		Version previousMaster = null;
		if (versionId != null) {
			version = changelog.getVersion(versionId);
			previousMaster = refLog.setMasterVersion(version).orElse(null);
		}

		try (Connection connection = backend.connect()) {
			try {
				connection.setAutoCommit(false);

				// Remove previous aliases
				if (previousMaster != null) {
					Collection<TableRef> tableRefs = refLog.getTableRefs(previousMaster);
					for (TableRef tableRef : tableRefs) {
						dropTableAlias(connection, tableRef);
					}
				}

				// Create new aliases.
				Collection<TableRef> tableRefs = refLog.getTableRefs(version);
				for (TableRef tableRef : tableRefs) {
					createTableAlias(connection, tableRef);
				}

				connection.commit();
			}
			catch (SQLException e) {
				connection.rollback();
				throw e;
			}
			finally {
				backend.persistState(state);
			}
		}
	}

	private void createTableAlias(Connection connection, TableRef tableRef) throws SQLException {
		String query = new QueryBuilder()
				.append("CREATE OR REPLACE VIEW " + tableRef.getName())
				.append("AS SELECT * FROM " + tableRef.getTableId() + ";")
				.toString();

		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
		}
	}

	private void dropTableAlias(Connection connection, TableRef tableRef) throws SQLException {
		String query = new QueryBuilder()
				.append("DROP VIEW " + tableRef.getName() + ";")
				.toString();

		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
		}
	}

}
