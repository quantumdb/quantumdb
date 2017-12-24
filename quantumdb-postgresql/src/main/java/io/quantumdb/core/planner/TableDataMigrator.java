package io.quantumdb.core.planner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.planner.MigratorFunction.Stage;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType.Type;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TableDataMigrator {

	private static final long BATCH_SIZE = 2_000;
	private static final long WAIT_TIME = 50;

	private final RefLog refLog;
	private final Backend backend;

	TableDataMigrator(Backend backend, RefLog refLog) {
		this.backend = backend;
		this.refLog = refLog;
	}

	void migrateData(NullRecords nullRecords, Table source, Table target, Version from, Version to,
			Set<String> migratedColumns, Set<String> columnsToMigrate) throws SQLException, InterruptedException {

		Map<String, Object> highestId = queryHighestId(source);
		if (highestId == null) {
			log.info("Table: {} is empty -> nothing target migrate...", source.getName());
			return;
		}
		log.info("Migrating data in table: {} target: {}", source.getName(), target.getName());

		MigratorFunction initialMigrator = SelectiveMigratorFunction.createMigrator(nullRecords, refLog,
				source, target, from, to, BATCH_SIZE, Stage.INITIAL, migratedColumns, columnsToMigrate);
		MigratorFunction successiveMigrator = SelectiveMigratorFunction.createMigrator(nullRecords, refLog,
				source, target, from, to, BATCH_SIZE, Stage.CONSECUTIVE, migratedColumns, columnsToMigrate);

		if (initialMigrator == null) {
			return;
		}

		try (Connection connection = backend.connect()) {
			execute(connection, initialMigrator.getCreateStatement());
			execute(connection, successiveMigrator.getCreateStatement());

			long start = System.currentTimeMillis();
			Map<String, Object> lastProcessedId = Maps.newHashMap();

			while (true) {
				long innerStart = System.currentTimeMillis();

				QueryBuilder migrator = new QueryBuilder();
				if (lastProcessedId.isEmpty()) {
					migrator.append("SELECT * FROM " + initialMigrator.getName() + "();");
				}
				else {
					List<String> values = successiveMigrator.getParameters().stream()
							.map(parameterName -> asExpression(lastProcessedId.get(stripEscaping(parameterName))))
							.collect(Collectors.toList());

					migrator.append("SELECT * FROM " + successiveMigrator.getName() + "(")
							.append(Joiner.on(", ").join(values) + ");");
				}

				try (Statement statement = connection.createStatement()) {
					ResultSet resultSet = statement.executeQuery(migrator.toString());

					if (resultSet.next()) {
						lastProcessedId.putAll(readIdentity(source, resultSet));
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
				log.info("Migration data source: {} target: {}, now at identity: {}, took: {} ms", source.getName(),
						target.getName(), lastProcessedId, innerEnd - innerStart);

				Thread.sleep(WAIT_TIME);
			}

			long end = System.currentTimeMillis();
			log.info("Migrating records source: {} target: {} took: {} ms", source.getName(), target.getName(), end - start);

			execute(connection, initialMigrator.getDropStatement());
			execute(connection, successiveMigrator.getDropStatement());
		}
	}

	private String stripEscaping(String parameterName) {
		if (parameterName.startsWith("\"") && parameterName.endsWith("\"")) {
			return parameterName.substring(1, parameterName.length() - 1);
		}
		return parameterName;
	}

	private Map<String, Object> queryHighestId(Table from) throws SQLException {
		List<String> identityColumns = from.getIdentityColumns().stream()
				.map(Column::getName)
				.collect(Collectors.toList());

		try (Connection connection = backend.connect()) {
			try (Statement statement = connection.createStatement()) {
				String query = new QueryBuilder()
						.append("SELECT " + Joiner.on(", ").join(identityColumns))
						.append("FROM " + from.getName())
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
				throw new IllegalStateException("NULL values in identity columns are currently not supported.");
			}

			if (!(left instanceof Comparable)) {
				left = left.toString();
			}

			if (!(right instanceof Comparable)) {
				right = right.toString();
			}

			if (left instanceof UUID) {
				left = left.toString().toLowerCase();
			}

			if (right instanceof UUID) {
				right = right.toString().toLowerCase();
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

	private Map<String, Object> readIdentity(Table from, ResultSet resultSet) throws SQLException {
		String result = resultSet.getString(1);
		result = result.substring(1, result.length() - 1);

		List<String> parts = Lists.newArrayList();
		StringBuilder currentPart = new StringBuilder();

		boolean quoted = false;
		char prev = ' ';
		for (int i = 0; i < result.length(); i++) {
			char c = result.charAt(i);
			if (c == ',' && !quoted) {
				parts.add(currentPart.toString());
				currentPart = new StringBuilder();
			}
			else if (c == '"' && prev != '\\') {
				quoted = !quoted;
			}
			else {
				currentPart.append(c);
			}
			prev = c;
		}

		if (currentPart.length() > 0) {
			parts.add(currentPart.toString());
		}

		Map<String, Object> identity = Maps.newHashMap();
		List<Column> identityColumns = from.getIdentityColumns();
		for (int i = 0; i < identityColumns.size(); i++) {
			Column column = identityColumns.get(i);
			String columnName = column.getName();
			Object value = parseValue(column.getType().getType(), parts.get(i));
			identity.put(columnName, value);
		}
		return identity;
	}

	private Object parseValue(Type type, String value) {
		if (Strings.isNullOrEmpty(value)) {
			return null;
		}

		switch (type) {
			case SMALLINT:
			case INTEGER:
				return Integer.parseInt(value);
			case BIGINT:
				return Long.parseLong(value);
			case BOOLEAN:
				return Boolean.parseBoolean(value);
			case TEXT:
			case CHAR:
			case VARCHAR:
				return value;
			case DATE:
				return Date.parse(value);
			case FLOAT:
				return Float.parseFloat(value);
			case TIMESTAMP:
				return Timestamp.parse(value);
			case OID:
				return value;
			case UUID:
			default:
				return UUID.fromString(value);
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
