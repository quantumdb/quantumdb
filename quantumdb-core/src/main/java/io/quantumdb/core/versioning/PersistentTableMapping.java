package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PersistentTableMapping extends TableMapping {

	private final Connection connection;

	@SneakyThrows
	public PersistentTableMapping(Changelog changelog, Connection connection) {
		this.connection = connection;

		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_tablemappings");

			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				Version version = changelog.getVersion(versionId);
				String tableName = resultSet.getString("table_name");
				String tableId = resultSet.getString("table_id");
				addInternally(version, tableName, tableId);
			}
		}
	}

	@Override
	protected void onPut(Version version, String tableName, TableMappingNode node) {
		insert(node.getTableId(), tableName, version);
	}

	@Override
	protected void onRemove(Version version, String tableName) {
		delete(tableName, version);
	}

	@SneakyThrows
	private void insert(String tableId, String tableName, Version version) {
		delete(tableName, version);
		String query = "INSERT INTO quantumdb_tablemappings (table_name, version_id, table_id) VALUES (?, ?, ?);";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, tableName);
			statement.setString(2, version.getId());
			statement.setString(3, tableId);
			statement.executeUpdate();
		}
	}

	@SneakyThrows
	private void delete(String tableName, Version version) {
		String query = "DELETE FROM quantumdb_tablemappings WHERE table_name = ? AND version_id = ?;";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, tableName);
			statement.setString(2, version.getId());
			statement.executeUpdate();
		}
	}

}
