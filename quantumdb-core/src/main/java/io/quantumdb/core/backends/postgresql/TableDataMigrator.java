package io.quantumdb.core.backends.postgresql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.postgresql.MigratorFunction.Stage;
import io.quantumdb.core.migration.utils.DataMapping;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.utils.QueryBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TableDataMigrator {

	public static enum Data {
		BASE, RELATIONAL;
	}

	private static final long BATCH_SIZE = 1_000;
	private static final long WAIT_TIME = 50;

	private final DataMapping mapping;
	private final Backend backend;

	TableDataMigrator(Backend backend, DataMapping mapping) {
		this.backend = backend;
		this.mapping = mapping;
	}

	void migrateData(Map<String, Identity> nullRecords, Data data) throws SQLException, InterruptedException {
		Map<String, Object> highestId = queryHighestId();
		if (highestId == null) {
			log.info("Table: {} is empty -> nothing to migrate...", mapping.getSourceTable().getName());
			return;
		}

		MigratorFunction initialMigratorFunction;
		MigratorFunction successiveMigratorFunction;

		switch (data) {
			case BASE:
				initialMigratorFunction = BaseMigratorFunction.create(nullRecords, mapping, BATCH_SIZE, Stage.INITIAL);
				successiveMigratorFunction = BaseMigratorFunction.create(nullRecords, mapping, BATCH_SIZE, Stage.CONSECUTIVE);
				break;
			case RELATIONAL:
				initialMigratorFunction = RelationalMigratorFunction.create(nullRecords, mapping, BATCH_SIZE, Stage.INITIAL);
				successiveMigratorFunction = RelationalMigratorFunction.create(nullRecords, mapping, BATCH_SIZE, Stage.CONSECUTIVE);
				break;
			default:
				throw new RuntimeException("Type: " + data + " is not supported!");
		}

		if (initialMigratorFunction == null) {
			return;
		}

		try (Connection connection = backend.connect()) {
			execute(connection, initialMigratorFunction.getCreateStatement());
			execute(connection, successiveMigratorFunction.getCreateStatement());
			execute(connection, "SET synchronous_commit TO off;");

			long start = System.currentTimeMillis();
			Map<String, Object> lastProcessedId = Maps.newHashMap();

			while (true) {
				long innerStart = System.currentTimeMillis();

				QueryBuilder migrator = new QueryBuilder();
				if (lastProcessedId.isEmpty()) {
					migrator.append("SELECT * FROM " + initialMigratorFunction.getName() + "();");
				}
				else {
					List<String> values = successiveMigratorFunction.getParameters().stream()
							.map(parameterName -> asExpression(lastProcessedId.get(parameterName)))
							.collect(Collectors.toList());

					migrator.append("SELECT * FROM " + successiveMigratorFunction.getName() + "(")
							.append(Joiner.on(", ").join(values) + ");");
				}

				try (Statement statement = connection.createStatement()) {
					ResultSet resultSet = statement.executeQuery(migrator.toString());

					if (resultSet.next()) {
						List<Column> identityColumns = mapping.getSourceTable().getIdentityColumns();
						for (int i = 0; i < identityColumns.size(); i++) {
							Column column = identityColumns.get(i);
							String columnName = column.getName();
							Object value = readValue(resultSet, i + 1, column.getType().getType());
							lastProcessedId.put(columnName, value);
						}

						if (greaterThanOrEqualsTo(lastProcessedId, highestId)) {
							break;
						}
					}
					else {
						// No records returned. We're done migrating data...
						break;
					}
				}

				long innerEnd = System.currentTimeMillis();
				log.info("Migration up to id: {} took: {} ms", lastProcessedId, innerEnd - innerStart);

				Thread.sleep(WAIT_TIME);
			}

			long end = System.currentTimeMillis();
			log.info("Migrating data from: {} to: {} took: {} ms", new Object[] {
					mapping.getSourceTable().getName(),
					mapping.getTargetTable().getName(),
					end - start
			});

			execute(connection, initialMigratorFunction.getDropStatement());
			execute(connection, successiveMigratorFunction.getDropStatement());
		}
	}

	private Map<String, Object> queryHighestId() throws SQLException {
		List<String> identityColumns = mapping.getSourceTable().getIdentityColumns().stream()
				.map(Column::getName)
				.collect(Collectors.toList());

		try (Connection connection = backend.connect()) {
			try (Statement statement = connection.createStatement()) {
				String query = new QueryBuilder()
						.append("SELECT " + Joiner.on(", ").join(identityColumns))
						.append("FROM " + mapping.getSourceTable().getName())
						.append("ORDER BY " + Joiner.on(" DESC, ").join(identityColumns) + " DESC")
						.append("LIMIT 1")
						.toString();

				ResultSet resultSet = statement.executeQuery(query);
				if (resultSet.next()) {
					Map<String, Object> id = Maps.newHashMap();
					for (String identityColumn : identityColumns) {
						Object value = resultSet.getObject(identityColumn);
						id.put(identityColumn, value);
					}
					return id;
				}
				return null;
			}
		}
	}

	private boolean greaterThanOrEqualsTo(Map<String, Object> cursor, Map<String, Object> limit) {
		for (String key : cursor.keySet()) {
			Object left = cursor.get(key);
			Object right = limit.get(key);

			if (left == null || right == null) {
				// TODO: Should account for NULLs in identity columns?
				throw new IllegalStateException("NULL values in identity columns are currently not supported.");
			}

			if (!(left instanceof Comparable)) {
				left = left.toString();
			}

			if (!(right instanceof Comparable)) {
				right = right.toString();
			}

			Comparable leftComparable = (Comparable) left;
			Comparable rightComparable = (Comparable) right;

			int compareTo = leftComparable.compareTo(rightComparable);
			if (compareTo < 0) {
				return false;
			}
		}
		return true;
	}

	private Object readValue(ResultSet resultSet, int index, ColumnType.Type type) throws SQLException {
		switch (type) {
			case SMALLINT:
			case INTEGER:
				return resultSet.getInt(index);
			case BIGINT:
				return resultSet.getLong(index);
			case BOOLEAN:
				return resultSet.getBoolean(index);
			case TEXT:
			case CHAR:
			case VARCHAR:
				return resultSet.getString(index);
			case DATE:
				return resultSet.getDate(index);
			case FLOAT:
				return resultSet.getFloat(index);
			case TIMESTAMP:
				return resultSet.getTimestamp(index);
			case OID:
			case UUID:
			default:
				return resultSet.getObject(index);
		}
	}

	private void execute(Connection connection, String query) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			log.debug("Executing: " + query);
			statement.execute(query);
		}
	}

	private String asExpression(Object value) {
		if (value instanceof Number) {
			return value.toString();
		}
		return "'" + value + "'";
	}

}
