package io.quantumdb.core.utils;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

public class BatchInserter {

	public static BatchInserter insertInto(Connection connection, String refId, String... columnNames)
			throws SQLException {

		return new BatchInserter(connection, refId, columnNames);
	}

	private final String[] columnNames;
	private final PreparedStatement statement;

	private BatchInserter(Connection connection, String refId, String... columnNames) throws SQLException {
		this.columnNames = columnNames;

		String parameters = Arrays.stream(columnNames)
				.map(columnName -> "?")
				.collect(Collectors.joining(", "));

		String names = Arrays.stream(columnNames)
				.collect(Collectors.joining(", "));

		this.statement = connection.prepareStatement("INSERT INTO " + refId + " (" + names + ") VALUES (" + parameters + ")");
	}

	public BatchInserter values(Object... values) throws SQLException {
		Preconditions.checkArgument(values != null);
		Preconditions.checkArgument(values.length == columnNames.length);

		for (int i = 0; i < values.length; i++) {
			Object value = values[i];
			statement.setObject(i + 1, value);
		}
		statement.addBatch();

		return this;
	}

	public void insert() throws SQLException {
		try {
			statement.executeBatch();
		}
		catch (BatchUpdateException e) {
			throw e.getNextException();
		}
		finally {
			statement.close();
		}
	}

}
