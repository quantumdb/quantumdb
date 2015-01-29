package io.quantumdb.core.backends.postgresql;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.name.Named;
import com.google.inject.persist.Transactional;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.migration.utils.DataMapping;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.ChangelogBackend;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.TableNameMappingBackend;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresqlBackend implements Backend {

	private final ChangelogBackend changelogBackend;
	private final TableNameMappingBackend tableNameMappingBackend;

	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPass;
	private final String jdbcCatalog;

	private Connection connection;

	@Inject
	PostgresqlBackend(ChangelogBackend changelogBackend, TableNameMappingBackend tableMappings,
			@Named("javax.persistence.jdbc.url") String jdbcUrl,
			@Named("javax.persistence.jdbc.user") String jdbcUser,
			@Named("javax.persistence.jdbc.password") String jdbcPass,
			@Named("javax.persistence.jdbc.catalog") String jdbcCatalog) {

		this.changelogBackend = changelogBackend;
		this.tableNameMappingBackend = tableMappings;

		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPass = jdbcPass;
		this.jdbcCatalog = jdbcCatalog;
	}

	@Override
	@Transactional
	public State loadState() throws SQLException {
		ensureConnected();

		log.trace("Loading state from database...");

		Changelog changelog = changelogBackend.load();
		Catalog catalog = new CatalogLoader(connection).load(jdbcCatalog);
		TableMapping tableMapping = tableNameMappingBackend.load(changelog);

		return new State(catalog, tableMapping, changelog);
	}

	@Override
	@Transactional
	public void persistState(State state) throws SQLException {
		log.info("Persisting state to database...");

		changelogBackend.persist(state.getChangelog());
		tableNameMappingBackend.persist(state.getChangelog(), state.getTableMapping());
	}

	@Override
	public void createTables(Collection<Table> tables) throws SQLException {
		ensureConnected();

		for (Table table : tables) {
			createTable(table);
		}
		for (Table table : tables) {
			createForeignKeys(table);
		}
	}

	@Override
	public void installDataMapping(DataMapping dataMapping) throws SQLException {
		ensureConnected();

		Table sourceTable = dataMapping.getSourceTable();
		String sourceTableName = sourceTable.getName();

		List<String> sourceColumnNames = Lists.newArrayList(dataMapping.getColumnMappings().keySet());

		List<String> targetColumnNames = sourceColumnNames.stream()
				.map(dataMapping.getColumnMappings()::get)
				.map(DataMapping.ColumnMapping::getColumnName)
				.collect(Collectors.toList());

		List<String> prefixedSourceColumnNames = sourceColumnNames.stream()
				.map(columnName -> "NEW." + columnName)
				.collect(Collectors.toList());

		String setClause = dataMapping.getColumnMappings().entrySet().stream()
				.map(entry -> entry.getValue().getColumnName() + " = NEW." + entry.getKey())
				.reduce((l, r) -> l + ", " + r)
				.orElseThrow(() -> new IllegalArgumentException("Cannot map 0 columns!"));

		String prefixedIdClass = dataMapping.getTargetTable().getIdentityColumns().stream()
				.map(column -> column.getName() + " = " + prefixedSourceColumnNames.get(targetColumnNames.indexOf(column.getName())))
				.reduce((l, r) -> l + " AND " + r)
				.orElseThrow(() -> new IllegalArgumentException("Cannot update table without identity columns!"));

		String idClausePrefixWithNew = dataMapping.getTargetTable().getIdentityColumns().stream()
				.map(column -> column.getName() + " = OLD." + sourceColumnNames.get(targetColumnNames.indexOf(column.getName())))
				.reduce((l, r) -> l + " AND " + r)
				.orElseThrow(() -> new IllegalArgumentException("Cannot update table without identity columns!"));

		String functionName = "sync_" + RandomHasher.generateHash();
		QueryBuilder functionBuilder = new QueryBuilder()
				.append("CREATE FUNCTION " + functionName + "()")
				.append("RETURNS TRIGGER AS $$")
				.append("BEGIN")
				.append("   IF TG_OP = 'INSERT' THEN")
				.append("       LOOP")
				.append("           UPDATE " + dataMapping.getTargetTable().getName())
				.append("               SET " + setClause)
				.append("               WHERE " + prefixedIdClass + ";")
				.append("           IF found THEN EXIT; END IF;")
				.append("           BEGIN")
				.append("               INSERT INTO " + dataMapping.getTargetTable().getName())
				.append("                   (" + Joiner.on(", ").join(targetColumnNames) + ") VALUES")
				.append("                   (" + Joiner.on(", ").join(prefixedSourceColumnNames) + ");")
				.append("               EXIT;")
				.append("           EXCEPTION WHEN unique_violation THEN")
				.append("           END;")
				.append("       END LOOP;")
				.append("   ELSIF TG_OP = 'UPDATE' THEN")
				.append("       LOOP")
				.append("           UPDATE " + dataMapping.getTargetTable().getName())
				.append("               SET " + setClause)
				.append("               WHERE " + prefixedIdClass + ";")
				.append("           IF found THEN EXIT; END IF;")
				.append("           BEGIN")
				.append("               INSERT INTO " + dataMapping.getTargetTable().getName())
				.append("                   (" + Joiner.on(", ").join(targetColumnNames) + ") VALUES")
				.append("                   (" + Joiner.on(", ").join(prefixedSourceColumnNames) + ");")
				.append("               EXIT;")
				.append("           EXCEPTION WHEN unique_violation THEN")
				.append("           END;")
				.append("       END LOOP;")
				.append("   ELSIF TG_OP = 'DELETE' THEN")
				.append("       DELETE FROM " + dataMapping.getTargetTable().getName())
				.append("           WHERE " + idClausePrefixWithNew + ";")
				.append("   END IF;")
				.append("   RETURN NEW;")
				.append("END;")
				.append("$$ LANGUAGE 'plpgsql';");

		String triggerName = "sync_" + RandomHasher.generateHash();
		QueryBuilder triggerBuilder = new QueryBuilder()
				.append("CREATE TRIGGER " + triggerName)
				.append("AFTER INSERT OR UPDATE OR DELETE")
				.append("ON " + sourceTable.getName())
				.append("FOR EACH ROW")
				.append("WHEN (pg_trigger_depth() = 0)")
				.append("EXECUTE PROCEDURE " + functionName + "();");

		log.info("Creating sync function: {} for table: {}", functionName, sourceTableName);
		execute(functionBuilder);

		log.info("Creating trigger: {} for table: {}", triggerName, sourceTableName);
		execute(triggerBuilder);
	}

	@Override
	public void migrateData(DataMapping dataMapping) throws SQLException, InterruptedException {
		new TableDataMigrator(jdbcUrl, jdbcUser, jdbcPass, dataMapping).migrateData();
	}

	private void createTable(Table table) throws SQLException {
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
				String sequenceName = column.getSequenceName();
				if (sequenceName == null) {
					sequenceName = table.getName() + "_" + column.getName() + "_seq";
					execute(new QueryBuilder().append("CREATE SEQUENCE " + sequenceName));
				}

				sequences.put(sequenceName, column.getName());
				queryBuilder.append("DEFAULT NEXTVAL('" + sequenceName + "')");
			}
			else if (!Strings.isNullOrEmpty(column.getDefaultValueExpression())) {
				queryBuilder.append("DEFAULT " + column.getDefaultValueExpression());
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

		execute(queryBuilder);

		for (Map.Entry<String, String> sequence : sequences.entrySet()) {
			execute(new QueryBuilder()
					.append("ALTER SEQUENCE " + sequence.getKey())
					.append("OWNED BY " + table.getName() + "." + sequence.getValue()));
		}
	}

	private void createForeignKeys(Table table) throws SQLException {
		for (ForeignKey foreignKey : table.getForeignKeys()) {
			String foreignKeyName = "fk_" + RandomHasher.generateHash();

			QueryBuilder queryBuilder = new QueryBuilder();
			queryBuilder.append("ALTER TABLE " + table.getName());
			queryBuilder.append("ADD CONSTRAINT " + foreignKeyName);
			queryBuilder.append("FOREIGN KEY (" + Joiner.on(", ").join(foreignKey.getReferencingColumns()) + ")");
			queryBuilder.append("REFERENCES " + foreignKey.getReferredTableName());
			queryBuilder.append("(" + Joiner.on(", ").join(foreignKey.getReferredColumns()) + ")");

			log.info("Creating foreign key: {}", foreignKeyName);
			execute(queryBuilder);
		}
	}

	private void ensureConnected() throws SQLException {
		if (connection != null && !connection.isClosed()) {
			return;
		}

		this.connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
	}

	private void execute(QueryBuilder queryBuilder) throws SQLException {
		ensureConnected();

		String query = queryBuilder.toString();
		try (Statement statement = connection.createStatement()) {
			log.debug("Executing: " + query);
			statement.execute(query);
		}
	}

}
