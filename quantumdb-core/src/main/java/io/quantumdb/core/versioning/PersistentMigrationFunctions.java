package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PersistentMigrationFunctions extends MigrationFunctions {

	private final Connection connection;

	@SneakyThrows
	public PersistentMigrationFunctions(Connection connection) {
		this.connection = connection;

		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_functions");

			while (resultSet.next()) {
				String sourceTableId = resultSet.getString("source_table_id");
				String targetTableId = resultSet.getString("target_table_id");
				String functionName = resultSet.getString("function_name");

				addFunction(sourceTableId, targetTableId, functionName);
			}
		}

		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_triggers");

			while (resultSet.next()) {
				String sourceTableId = resultSet.getString("source_table_id");
				String targetTableId = resultSet.getString("target_table_id");
				String triggerName = resultSet.getString("trigger_name");

				addTrigger(sourceTableId, targetTableId, triggerName);
			}
		}
	}

	@Override
	protected void onFunctionPut(String sourceTableId, String targetTableId, String functionName) {
		insertFunction(sourceTableId, targetTableId, functionName);
	}

	@Override
	protected void onFunctionRemove(String sourceTableId, String targetTableId) {
		deleteFunction(sourceTableId, targetTableId);
	}

	@Override
	protected void onTriggerPut(String sourceTableId, String targetTableId, String triggerName) {
		insertTrigger(sourceTableId, targetTableId, triggerName);
	}

	@Override
	protected void onTriggerRemove(String sourceTableId, String targetTableId) {
		deleteTrigger(sourceTableId, targetTableId);
	}

	@SneakyThrows
	private void insertFunction(String sourceTableId, String targetTableId, String functionName) {
		deleteFunction(sourceTableId, targetTableId);
		String query = "INSERT INTO quantumdb_functions (source_table_id, target_table_id, function_name) VALUES (?, ?, ?);";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, sourceTableId);
			statement.setString(2, targetTableId);
			statement.setString(3, functionName);
			statement.executeUpdate();
		}
	}

	@SneakyThrows
	private void insertTrigger(String sourceTableId, String targetTableId, String triggerName) {
		deleteTrigger(sourceTableId, targetTableId);
		String query = "INSERT INTO quantumdb_triggers (source_table_id, target_table_id, trigger_name) VALUES (?, ?, ?);";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, sourceTableId);
			statement.setString(2, targetTableId);
			statement.setString(3, triggerName);
			statement.executeUpdate();
		}
	}

	@SneakyThrows
	private void deleteFunction(String sourceTableId, String targetTableId) {
		String query = "DELETE FROM quantumdb_functions WHERE source_table_id = ? AND target_table_id = ?;";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, sourceTableId);
			statement.setString(2, targetTableId);
			statement.executeUpdate();
		}
	}

	@SneakyThrows
	private void deleteTrigger(String sourceTableId, String targetTableId) {
		String query = "DELETE FROM quantumdb_triggers WHERE source_table_id = ? AND target_table_id = ?;";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, sourceTableId);
			statement.setString(2, targetTableId);
			statement.executeUpdate();
		}
	}

}
