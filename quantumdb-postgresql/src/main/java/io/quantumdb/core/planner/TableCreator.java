package io.quantumdb.core.planner;

import static io.quantumdb.core.planner.QueryUtils.execute;
import static io.quantumdb.core.planner.QueryUtils.quoted;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.ForeignKey.Action;
import io.quantumdb.core.schema.definitions.Index;
import io.quantumdb.core.schema.definitions.Sequence;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TableCreator {

	private final Config config;

	public void create(Connection connection, Collection<Table> tables) throws SQLException {
		createTables(connection, tables);
		createForeignKeys(connection, tables);
		createIndexes(connection, tables);
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

	public void createIndexes(Connection connection, Collection<Table> tables) throws SQLException {
		for (Table table : tables) {
			createIndexes(connection, table);
		}
	}

	private void createTable(Connection connection, Table table) throws SQLException {
		log.info("Creating table: {}", table.getName());
		Map<String, String> sequences = Maps.newHashMap();

		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.append("CREATE TABLE " + quoted(table.getName()) + " (");

		boolean shouldOwnSequence = false;
		boolean columnAdded = false;
		for (Column column : table.getColumns()) {
			if (columnAdded) {
				queryBuilder.append(", ");
			}

			queryBuilder.append(quoted(column.getName()) + " " + column.getType());
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

					shouldOwnSequence = true;
					execute(connection, config, "CREATE SEQUENCE " + quoted(sequenceName) + ";");
				}

				sequences.put(sequence.getName(), column.getName());
				queryBuilder.append("DEFAULT NEXTVAL('" + quoted(sequence.getName()) + "')");
			}
			else if (!Strings.isNullOrEmpty(column.getDefaultValue())) {
				queryBuilder.append("DEFAULT " + column.getDefaultValue());
			}

			columnAdded = true;
		}

		List<String> primaryKeyColumns = table.getPrimaryKeyColumns().stream()
				.map(Column::getName)
				.collect(Collectors.toList());

		if (!primaryKeyColumns.isEmpty()) {
			queryBuilder.append(", PRIMARY KEY(" + primaryKeyColumns.stream().map(QueryUtils::quoted).collect(Collectors.joining(", ")) + ")");
		}

		queryBuilder.append(");");

		execute(connection, config, queryBuilder.toString());

		if (shouldOwnSequence) {
			for (Map.Entry<String, String> sequence : sequences.entrySet()) {
				execute(connection, config, new QueryBuilder()
						.append("ALTER SEQUENCE " + quoted(sequence.getKey()))
						.append("OWNED BY " + quoted(table.getName()) + "." + quoted(sequence.getValue()) + ";")
						.toString());
			}
		}
	}

	private void createForeignKeys(Connection connection, Table table) throws SQLException {
		for (ForeignKey foreignKey : table.getForeignKeys()) {
			QueryBuilder queryBuilder = new QueryBuilder();
			queryBuilder.append("ALTER TABLE " + quoted(table.getName()));
			queryBuilder.append("ADD CONSTRAINT " + quoted(foreignKey.getForeignKeyName()));
			queryBuilder.append("FOREIGN KEY (" + foreignKey.getReferencingColumns().stream().map(QueryUtils::quoted).collect(Collectors.joining(", ")) + ")");
			queryBuilder.append("REFERENCES " + quoted(foreignKey.getReferredTableName()));
			queryBuilder.append("(" + foreignKey.getReferredColumns().stream().map(QueryUtils::quoted).collect(Collectors.joining(", ")) + ")");
			queryBuilder.append("ON UPDATE " + valueOf(foreignKey.getOnUpdate()));
			queryBuilder.append("ON DELETE " + valueOf(foreignKey.getOnDelete()));
			queryBuilder.append("DEFERRABLE");

			log.info("Creating foreign key: {}", foreignKey.getForeignKeyName());
			execute(connection, config, queryBuilder.toString());
		}
	}

	private void createIndexes(Connection connection, Table table) throws SQLException {
		for (Index index : table.getIndexes()) {
			QueryBuilder queryBuilder = new QueryBuilder();
			queryBuilder.append("CREATE");
			if (index.isUnique()) {
				queryBuilder.append("UNIQUE");
			}
			queryBuilder.append("INDEX " + quoted(index.getIndexName()));
			queryBuilder.append("ON " + quoted(index.getParent().getName()));
			queryBuilder.append("(" + index.getColumns().stream().map(QueryUtils::quoted).collect(Collectors.joining(", ")) + ");");

			log.info("Creating index key: {} ({})", index.getIndexName(), index.getColumns());
			execute(connection, config, queryBuilder.toString());
		}
	}

	private String valueOf(Action action) {
		switch (action) {
			case CASCADE: return "CASCADE";
			case NO_ACTION: return "NO ACTION";
			case RESTRICT: return "RESTRICT";
			case SET_DEFAULT: return "SET DEFAULT";
			case SET_NULL: return "SET NULL";
			default: throw new IllegalArgumentException("Action: " + action + " is not supported!");
		}
	}

}
