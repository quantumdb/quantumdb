package io.quantumdb.core.backends.postgresql.migrator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Sequence;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NullRecords {

	private final Map<Table, Identity> identities = Maps.newHashMap();

	public Map<Table, Identity> insertNullObjects(Backend backend, Set<Table> tables) throws SQLException {
		for (Table table : tables) {
			Preconditions.checkArgument(!identities.containsKey(table));
		}

		Map<Table, Identity> persisted = Maps.newHashMap();

		try (Connection connection = backend.connect()) {
			ensureDeferredConstraints(connection);

			Set<String> tableNames = tables.stream()
					.map(Table::getName)
					.collect(Collectors.toSet());

			log.debug("Generating NULL objects for tables: " + tableNames);
			Map<String, Identity> generatedIdentities = Maps.newHashMap();
			for (Table table : tables) {
				Identity identity = insertNullObject(connection, table, generatedIdentities);
				if (identity != null) {
					persisted.put(table, identity);
				}
			}

			commit(connection);
		}

		identities.putAll(persisted);
		return persisted;
	}

	public void deleteNullObjects(Backend backend, Set<Table> tables) throws SQLException {
		for (Table table : tables) {
			Preconditions.checkArgument(identities.containsKey(table));
		}

		try (Connection connection = backend.connect()) {
			ensureDeferredConstraints(connection);

			log.debug("Dropping NULL objects for tables: " + tables);
			for (Table table : tables) {
				dropNullObject(connection, table);
			}

			commit(connection);
		}
	}

	public boolean hasNullRecord(Table table) {
		return identities.containsKey(table);
	}

	public Identity getIdentity(Table table) {
		return identities.get(table);
	}

	private void dropNullObject(Connection connection, Table table) throws SQLException {
		Identity identity = identities.get(table);

		QueryBuilder queryBuilder = new QueryBuilder("DELETE FROM " + table.getName() + " WHERE");
		queryBuilder.append(Joiner.on(" = ?, ").join(identity.keys()) + " = ?");

		try (PreparedStatement statement = connection.prepareStatement(queryBuilder.toString())) {
			int i = 0;
			for (String columnName : identity.keys()) {
				i++;
				Object value = identity.getValue(columnName);
				Column column = table.getColumn(columnName);
				ColumnType type = column.getType();
				type.getValueSetter().setValue(statement, i, value);
			}
			statement.execute();
		}
	}

	private Identity insertNullObject(Connection connection, Table table, Map<String, Identity> generatedIdentities) throws SQLException {
		List<Column> columnsToSet = table.getColumns().stream()
				.filter(column -> column.isIdentity() || column.isNotNull())
				.collect(Collectors.toList());

		List<String> columnNames = columnsToSet.stream()
				.map(Column::getName)
				.collect(Collectors.toList());

		List<String> columnValues = columnsToSet.stream()
				.map(column -> "?")
				.collect(Collectors.toList());

		QueryBuilder builder = new QueryBuilder();

		Identity identity = generateIdentity(connection, table, generatedIdentities);
		if (columnNames.isEmpty()) {
			builder.append("INSERT INTO")
					.append(table.getName())
					.append("DEFAULT VALUES;");
		}
		else {
			builder.append("INSERT INTO")
					.append(table.getName())
					.append("(" + Joiner.on(", ").join(columnNames) + ")")
					.append("VALUES")
					.append("(" + Joiner.on(", ").join(columnValues) + ");");
		}

		Map<String, Object> values = Maps.newLinkedHashMap();
		try (PreparedStatement statement = connection.prepareStatement(builder.toString())) {
			for (int i = 1; i <= columnsToSet.size(); i++) {
				Column column = columnsToSet.get(i - 1);
				ColumnType.ValueSetter valueSetter = column.getType().getValueSetter();
				String columnName = column.getName();
				ForeignKey outgoingForeignKey = column.getOutgoingForeignKey();

				if (column.isIdentity()) {
					Object value = identity.getValue(columnName);
					valueSetter.setValue(statement, i, value);
					values.put(column.getName(), value);
				}
				else if (outgoingForeignKey != null) {
					Identity referredIdentity = generateIdentity(connection, outgoingForeignKey.getReferredTable(), generatedIdentities);
					String referredColumnName = outgoingForeignKey.getColumns().get(columnName);
					Object value = referredIdentity.getValue(referredColumnName);
					valueSetter.setValue(statement, i, value);
					values.put(column.getName(), value);
				}
				else {
					ColumnType.ValueGenerator valueGenerator = column.getType().getValueGenerator();
					Object value = valueGenerator.generateValue();
					valueSetter.setValue(statement, i, value);
					values.put(column.getName(), value);
				}
			}

			log.debug("Inserted " + table.getName() + " - " + values);

			statement.execute();
		}
		catch (SQLException e) {
			log.error("Error while executing query: " + builder.toString() + " - " + e.getMessage(), e);
			throw e;
		}
		return identity;
	}

	private Identity generateIdentity(Connection connection, Table table, Map<String, Identity> generatedIdentities) throws SQLException {
		if (generatedIdentities.containsKey(table.getName())) {
			return generatedIdentities.get(table.getName());
		}

		Identity identity = new Identity();
		generatedIdentities.put(table.getName(), identity);

		for (Column identityColumn : table.getIdentityColumns()) {
			ForeignKey outgoingForeignKey = identityColumn.getOutgoingForeignKey();
			if (identityColumn.isAutoIncrement()) {
				Sequence sequence = identityColumn.getSequence();

				try (Statement statement = connection.createStatement()) {
					ResultSet resultSet = statement.executeQuery("SELECT NEXTVAL('" + sequence.getName() + "') AS val");
					if (resultSet.next()) {
						Object value = resultSet.getLong("val");
						identity.add(identityColumn.getName(), value);
					}
				}
			}
			else if (outgoingForeignKey != null) {
				Map<String, String> columns = outgoingForeignKey.getColumns();
				String mappedColumnName = columns.get(identityColumn.getName());
				Identity referredIdentity = generateIdentity(connection, outgoingForeignKey.getReferredTable(), generatedIdentities);
				identity.add(identityColumn.getName(), referredIdentity.getValue(mappedColumnName));
			}
			else {
				ColumnType.ValueGenerator valueGenerator = identityColumn.getType().getValueGenerator();
				Object value = valueGenerator.generateValue();
				identity.add(identityColumn.getName(), value);
			}
		}

		return identity;
	}

	private void ensureDeferredConstraints(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		try (Statement statement = connection.createStatement()) {
			statement.execute("SET CONSTRAINTS ALL DEFERRED;");
		}
	}

	private void commit(Connection connection) throws SQLException {
		connection.commit();
		connection.setAutoCommit(true);
	}
}
