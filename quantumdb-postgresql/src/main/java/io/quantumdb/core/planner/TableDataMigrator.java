package io.quantumdb.core.planner;

import static io.quantumdb.core.planner.QueryUtils.execute;
import static io.quantumdb.core.planner.QueryUtils.quoted;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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
import io.quantumdb.core.backends.Config;
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

	private final Backend backend;
	private final Config config;
	private final RefLog refLog;

	TableDataMigrator(Backend backend, Config config, RefLog refLog) {
		this.backend = backend;
		this.config = config;
		this.refLog = refLog;
	}

	void migrateData(NullRecords nullRecords, Table source, Table target, Version from, Version to,
			Set<String> migratedColumns, Set<String> columnsToMigrate) throws SQLException, InterruptedException {

		Map<String, Object> highestId = queryHighestId(source);
		if (highestId == null) {
			log.info("Table: {} is empty -> nothing to migrate...", source.getName());
			return;
		}
		log.info("Migrating data in table: {} to target: {}", source.getName(), target.getName());

		MigratorFunction initialMigrator = SelectiveMigratorFunction.createMigrator(nullRecords, refLog,
				source, target, from, to, BATCH_SIZE, Stage.INITIAL, migratedColumns, columnsToMigrate);
		MigratorFunction successiveMigrator = SelectiveMigratorFunction.createMigrator(nullRecords, refLog,
				source, target, from, to, BATCH_SIZE, Stage.CONSECUTIVE, migratedColumns, columnsToMigrate);

		if (initialMigrator == null) {
			return;
		}

		try (Connection connection = backend.connect()) {
			execute(connection, config, initialMigrator.getCreateStatement());
			execute(connection, config, successiveMigrator.getCreateStatement());

			if (!config.isDryRun()) {
				migrate(connection, source, target, highestId, initialMigrator, successiveMigrator);
			}

			execute(connection, config, initialMigrator.getDropStatement());
			execute(connection, config, successiveMigrator.getDropStatement());
		}
	}

	private void migrate(Connection connection, Table source, Table target, Map<String, Object> highestId,
			MigratorFunction initialMigrator, MigratorFunction successiveMigrator)
			throws SQLException, InterruptedException {

		long start = System.currentTimeMillis();
		Map<String, Object> lastProcessedId = Maps.newHashMap();

		while (true) {
			long innerStart = System.currentTimeMillis();

			QueryBuilder migrator = new QueryBuilder();
			if (lastProcessedId.isEmpty()) {
				migrator.append("SELECT * FROM " + quoted(initialMigrator.getName()) + "();");
			}
			else {
				List<String> values = successiveMigrator.getParameters().keySet().stream()
						.map(parameterName -> asExpression(lastProcessedId.get(stripEscaping(parameterName))) + "::" + successiveMigrator.getParameters().get(parameterName))
						.collect(Collectors.toList());

				migrator.append("SELECT * FROM " + quoted(successiveMigrator.getName()) + "(")
						.append(Joiner.on(", ").join(values) + ");");
			}

			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery(migrator.toString());
				if (resultSet.next()) {
					Map<String, Object> identityMap = readIdentity(source, resultSet);
					if (identityMap.keySet().isEmpty()) {
						break;
					}

					lastProcessedId.putAll(identityMap);
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
	}

	private String stripEscaping(String parameterName) {
		if (parameterName.startsWith("\"") && parameterName.endsWith("\"")) {
			return parameterName.substring(1, parameterName.length() - 1);
		}
		return parameterName;
	}

	private Map<String, Object> queryHighestId(Table from) throws SQLException {
		List<String> primaryKeyColumns = from.getPrimaryKeyColumns().stream()
				.map(Column::getName)
				.collect(Collectors.toList());

		try (Connection connection = backend.connect()) {
			try (Statement statement = connection.createStatement()) {
				String query = new QueryBuilder()
						.append("SELECT " + primaryKeyColumns.stream().map(QueryUtils::quoted).collect(Collectors.joining(", ")))
						.append("FROM " + quoted(from.getName()))
						.append("ORDER BY " + primaryKeyColumns.stream().map(value -> quoted(value) + " DESC").collect(Collectors.joining(", ")))
						.append("LIMIT 1")
						.toString();

				ResultSet resultSet = statement.executeQuery(query);
				if (resultSet.next()) {
					Map<String, Object> id = Maps.newHashMap();
					for (String primaryKeyColumn : primaryKeyColumns) {
						Object value = parseValue(from.getColumn(primaryKeyColumn).getType().getType(),resultSet.getObject(primaryKeyColumn).toString());
						id.put(primaryKeyColumn, value);
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
				throw new IllegalStateException("NULL values in primary key columns are currently not supported.");
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
		List<Column> primaryKeyColumns = from.getPrimaryKeyColumns();

		if (parts.size() != primaryKeyColumns.size()) {
			return identity;
		}

		for (int i = 0; i < primaryKeyColumns.size(); i++) {
			Column column = primaryKeyColumns.get(i);
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
				return Short.parseShort(value);
			case INTEGER:
				return Integer.parseInt(value);
			case BIGINT:
				return Long.parseLong(value);
			case FLOAT:
				return Float.parseFloat(value);
			case DOUBLE:
				return Double.parseDouble(value);
			case NUMERIC:
				return new BigDecimal(value);
			case BOOLEAN:
				return Boolean.parseBoolean(value);
			case TEXT:
			case CHAR:
			case VARCHAR:
			case OID:
				return value;
			case DATE:
				return Date.valueOf(value);
			case TIMESTAMP:
				return Timestamp.valueOf(value);
			case UUID:
			default:
				return UUID.fromString(value);
		}
	}

	private String asExpression(Object value) {
		if (value instanceof Number) {
			return value.toString();
		}
		return "'" + value + "'";
	}

}
