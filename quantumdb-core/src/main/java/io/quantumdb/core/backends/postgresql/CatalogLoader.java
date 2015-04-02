package io.quantumdb.core.backends.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Sequence;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;

class CatalogLoader {

	private static final Pattern SEQUENCE_EXPRESSION = Pattern.compile("nextval\\(\\'(\\w+_id_seq)\\'::regclass\\)", Pattern.CASE_INSENSITIVE);

	private final Connection connection;

	CatalogLoader(Connection connection) {
		this.connection = connection;
	}

	Catalog load(String catalogName) throws SQLException {
		Catalog catalog = new Catalog(catalogName);
		loadTables(catalog);
		return catalog;
	}

	private void loadTables(Catalog catalog) throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT DISTINCT(table_name) AS table_name ")
				.append("FROM information_schema.columns ")
				.append("WHERE table_schema = ? ")
				.append("ORDER BY table_name ASC")
				.toString();

		Set<String> tableNames = Sets.newHashSet();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, "public");

			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String tableName = resultSet.getString("table_name");
				if (tableName.startsWith("quantumdb_")) {
					// Skip quantumdb related tables, since these are versioned internally...
					continue;
				}

				tableNames.add(tableName);
			}
		}

		for (String tableName : tableNames) {
			catalog.addTable(createTable(tableName));
		}

		for (String tableName : tableNames) {
			addForeignKeys(catalog, tableName);
		}
	}

	private Table createTable(String tableName) throws SQLException {
		Table table = new Table(tableName);

		String query = new QueryBuilder()
				.append("SELECT *")
				.append("FROM information_schema.columns")
				.append("WHERE table_schema = ? AND table_name = ?")
				.append("ORDER BY ordinal_position ASC")
				.toString();

		List<Column> columns = Lists.newArrayList();
		Set<String> primaryKeys = determinePrimaryKeys(tableName);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, "public");
			statement.setString(2, tableName);

			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String columnName = resultSet.getString("column_name");
				String expression = resultSet.getString("column_default");
				String type = resultSet.getString("data_type");
				Integer characterMaximum = null;
				if (resultSet.getObject("character_maximum_length") != null) {
					characterMaximum = resultSet.getInt("character_maximum_length");
				}

				Set<Column.Hint> hints = Sets.newHashSet();
				if (!"yes".equalsIgnoreCase(resultSet.getString("is_nullable"))) {
					hints.add(Column.Hint.NOT_NULL);
				}
				if (primaryKeys.contains(columnName) || (primaryKeys.isEmpty() && columns.isEmpty())) { // TODO: <--- HACK!!
					hints.add(Column.Hint.IDENTITY);
				}

				Sequence sequence = null;
				if (expression != null) {
					Matcher matcher = SEQUENCE_EXPRESSION.matcher(expression);
					if (matcher.find()) {
						hints.add(Column.Hint.AUTO_INCREMENT);
						sequence = new Sequence(matcher.group(1));
					}
				}

				Column.Hint[] hintArray = hints.stream().toArray(Column.Hint[]::new);

				Column column;
				if (sequence == null) {
					column = new Column(columnName, PostgresTypes.from(type, characterMaximum), expression, hintArray);
				}
				else {
					column = new Column(columnName, PostgresTypes.from(type, characterMaximum), sequence, hintArray);
				}
				columns.add(column);
			}
		}

		return table.addColumns(columns);
	}

	private Set<String> determinePrimaryKeys(String tableName) throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT ")
				.append("  pg_attribute.attname AS name ")
				.append("FROM pg_index, pg_class, pg_attribute, pg_namespace ")
				.append("WHERE ")
				.append("  nspname = 'public' AND ")
				.append("  pg_class.oid = '" + tableName + "'::regclass AND ")
				.append("  indrelid = pg_class.oid AND ")
				.append("  pg_class.relnamespace = pg_namespace.oid AND ")
				.append("  pg_attribute.attrelid = pg_class.oid AND ")
				.append("  pg_attribute.attnum = any(pg_index.indkey) ")
				.append(" AND indisprimary")
				.toString();

		Set<String> primaryKeyColumns = Sets.newLinkedHashSet();
		try (Statement statement = connection.createStatement()) {

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String name = resultSet.getString("name");
				primaryKeyColumns.add(name);
			}
		}

		return primaryKeyColumns;
	}

	private void addForeignKeys(Catalog catalog, String tableName) throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT")
				.append("  tc.constraint_name AS constraint_name,")
				.append("  kcu.column_name AS referring_column_name, ")
				.append("  ccu.table_name AS referenced_table_name,")
				.append("  ccu.column_name AS referenced_column_name")
				.append("FROM information_schema.table_constraints tc")
				.append("JOIN information_schema.key_column_usage kcu")
				.append("  ON tc.constraint_name = kcu.constraint_name")
				.append("JOIN information_schema.constraint_column_usage ccu")
				.append("  ON ccu.constraint_name = tc.constraint_name")
				.append("WHERE constraint_type = 'FOREIGN KEY'")
				.append("  AND kcu.table_schema = ? AND kcu.table_name = ?")
				.toString();

		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, "public");
			statement.setString(2, tableName);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
//				String constraintName = resultSet.getString("constraint_name");
				String referringColumnName = resultSet.getString("referring_column_name");
				String referencedTableName = resultSet.getString("referenced_table_name");
				String referencedColumnName = resultSet.getString("referenced_column_name");

				Table referringTable = catalog.getTable(tableName);
				Table referencedTable = catalog.getTable(referencedTableName);

				referringTable.addForeignKey(referringColumnName)
						.referencing(referencedTable, referencedColumnName);
			}
		}
	}
}
