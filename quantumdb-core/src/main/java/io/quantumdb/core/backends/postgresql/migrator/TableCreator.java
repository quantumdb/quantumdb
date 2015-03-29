package io.quantumdb.core.backends.postgresql.migrator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Sequence;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.utils.RandomHasher;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableCreator {

	public void create(Connection connection, Collection<Table> tables) throws SQLException {
		createTables(connection, tables);
		createForeignKeys(connection, tables);
	}

	public void createTables(Connection connection, Collection<Table> tables) throws SQLException {
		for (Table table : tables) {
			createTable(connection, table);
		}
	}

	public void createForeignKeys(Connection connection, Collection<Table> tables) throws SQLException {
		for (Table table : tables) {
			createForeignKeys(connection, table);
		}
	}

	private void createTable(Connection connection, Table table) throws SQLException {
		log.info("Creating table: {}", table.getName());
		Map<String, String> sequences = Maps.newHashMap();

		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.append("CREATE TABLE " + table.getName() + " (");

		boolean columnAdded = false;
		for (Column column : table.getColumns()) {
			if (columnAdded) {
				queryBuilder.append(", ");
			}

			queryBuilder.append(column.getName() + " " + column.getType());
			if (column.isNotNull()) {
				queryBuilder.append("NOT NULL");
			}

			if (column.isAutoIncrement()) {
				Sequence sequence = column.getSequence();
				if (sequence == null) {
					String sequenceName = table.getName() + "_" + column.getName() + "_seq";
					sequence = new Sequence(sequenceName);
					table.getParent().addSequence(sequence);
					column.modifyDefaultValue(sequence);

					execute(connection, new QueryBuilder("CREATE SEQUENCE " + sequenceName + ";"));
				}

				sequences.put(sequence.getName(), column.getName());
				queryBuilder.append("DEFAULT NEXTVAL('" + sequence.getName() + "')");
			}
			else if (!Strings.isNullOrEmpty(column.getDefaultValue())) {
				queryBuilder.append("DEFAULT " + column.getDefaultValue());
			}

			columnAdded = true;
		}

		List<String> identityColumns = table.getIdentityColumns().stream()
				.map(Column::getName)
				.collect(Collectors.toList());

		if (!identityColumns.isEmpty()) {
			queryBuilder.append(", PRIMARY KEY(" + Joiner.on(", ").join(identityColumns) + ")");
		}

		queryBuilder.append(")");

		execute(connection, queryBuilder);

		for (Map.Entry<String, String> sequence : sequences.entrySet()) {
			execute(connection, new QueryBuilder()
					.append("ALTER SEQUENCE " + sequence.getKey())
					.append("OWNED BY " + table.getName() + "." + sequence.getValue()));
		}
	}

	private void createForeignKeys(Connection connection, Table table) throws SQLException {
		for (ForeignKey foreignKey : table.getForeignKeys()) {
			String foreignKeyName = "fk_" + RandomHasher.generateHash();

			QueryBuilder queryBuilder = new QueryBuilder();
			queryBuilder.append("ALTER TABLE " + table.getName());
			queryBuilder.append("ADD CONSTRAINT " + foreignKeyName);
			queryBuilder.append("FOREIGN KEY (" + Joiner.on(", ").join(foreignKey.getReferencingColumns()) + ")");
			queryBuilder.append("REFERENCES " + foreignKey.getReferredTableName());
			queryBuilder.append("(" + Joiner.on(", ").join(foreignKey.getReferredColumns()) + ") ");
			queryBuilder.append("DEFERRABLE");

			log.info("Creating foreign key: {}", foreignKeyName);
			execute(connection, queryBuilder);
		}
	}

	private void execute(Connection connection, QueryBuilder queryBuilder) throws SQLException {
		String query = queryBuilder.toString();
		try (Statement statement = connection.createStatement()) {
			log.debug("Executing: " + query);
			statement.execute(query);
		}
		catch (SQLException e) {
			throw new SQLException(query, e);
		}
	}

}
