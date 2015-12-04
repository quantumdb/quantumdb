package io.quantumdb.core.backends.postgresql;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table.Cell;
import io.quantumdb.core.backends.postgresql.MigratorFunction.Stage;
import io.quantumdb.core.backends.postgresql.migrator.NullRecords;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.state.RefLog;
import io.quantumdb.core.state.RefLog.TableRef;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SelectiveMigratorFunction {

	static MigratorFunction createMigrator(NullRecords nullRecords, RefLog refLog, Table source, Table target,
			Version from, Version to, long batchSize, Stage stage, Set<String> migratedColumns,
			Set<String> columnsToBeMigrated) {

		if (migratedColumns.isEmpty()) {
			return createInsertMigrator(nullRecords, refLog, source, target, from, to, batchSize, stage, columnsToBeMigrated);
		}
		else {
			return createUpdateMigrator(refLog, source, target, from, to, batchSize, stage, columnsToBeMigrated);
		}
	}

	private static MigratorFunction createUpdateMigrator(RefLog refLog, Table source, Table target, Version from,
			Version to, long batchSize, Stage stage, Set<String> columnsToBeMigrated) {

		List<Column> identityColumns = source.getIdentityColumns();
		Map<String, String> functionParameterMapping = Maps.newHashMap();
		for (int i = 0; i < identityColumns.size(); i++) {
			functionParameterMapping.put(identityColumns.get(i).getName(), "q" + i);
		}

		List<String> identityColumnNames = identityColumns.stream()
				.map(column -> "\"" + column.getName() + "\"")
				.collect(Collectors.toList());

		List<String> functionParameters = identityColumns.stream()
				.map(column -> functionParameterMapping.get(column.getName()) + " " + column.getType().toString())
				.collect(Collectors.toList());

		String functionName = "migrator_" + RandomHasher.generateHash();

		QueryBuilder createStatement = new QueryBuilder();

		switch (stage) {
			case INITIAL:
				createStatement.append("CREATE FUNCTION " + functionName + "()");
				break;
			case CONSECUTIVE:
				createStatement.append(
						"CREATE FUNCTION " + functionName + "(" + Joiner.on(", ").join(functionParameters) + ")");
				break;
		}

		createStatement.append("  RETURNS text AS $$");
		createStatement.append("  DECLARE r record;");
		createStatement.append("  BEGIN");
		createStatement.append("	FOR r IN");
		createStatement.append("	  SELECT * FROM " + source.getName());

		if (stage != Stage.INITIAL) {
			createStatement.append("		WHERE");
			for (int i = 0; i < identityColumns.size(); i++) {
				if (i > 0) {
					createStatement.append("OR");
				}

				createStatement.append("(");

				for (int j = 0; j < i; j++) {
					String identityColumnName = identityColumns.get(j).getName();
					String value = functionParameterMapping.get(identityColumnName);
					createStatement.append(identityColumnName + " = " + value);
					createStatement.append("AND");
				}

				String identityColumnName = identityColumns.get(i).getName();
				String value = functionParameterMapping.get(identityColumnName);
				createStatement.append(identityColumnName + " > " + value);
				createStatement.append(")");
			}
		}

		refLog.getMapping(source, target);

		Map<String, String> columnsToMigrate = mapping.getColumnMappings().cellSet().stream()
				.filter(cell -> columnsToBeMigrated.contains(cell.getColumnKey()))
				.map(entry -> {
					String newColumnName = entry.getColumnKey();
					Column newColumn = target.getColumn(newColumnName);
					return new SimpleImmutableEntry<>(newColumn, entry.getRowKey());
				})
				.collect(Collectors.toMap(entry -> "\"" + entry.getKey().getName() + "\"",
						entry -> "\"" + entry.getValue() + "\""));

		if (columnsToMigrate.isEmpty()) {
			return null;
		}

		String updates = columnsToMigrate.keySet().stream()
				.map(columnName -> {
					String oldColumnName = columnsToMigrate.get(columnName);
					return columnName + " = r." + oldColumnName;
				})
				.collect(Collectors.joining(", "));

		String identityCondition = mapping.getSourceTable().getIdentityColumns().stream()
				.map(column -> {
					com.google.common.collect.Table<String, String, Transformation> columnMappings = mapping.getColumnMappings();
					Set<String> mappedColumnNames = columnMappings.row(column.getName()).keySet();
					String mappedColumnName = mappedColumnNames.iterator().next();
					return "\"" + mappedColumnName + "\" = r.\"" + column.getName() + "\"";
				})
				.collect(Collectors.joining(" AND "));

		createStatement.append("		ORDER BY " + Joiner.on(" ASC, ").join(identityColumnNames) + " ASC");
		createStatement.append("		LIMIT " + batchSize);
		createStatement.append("	LOOP");
		createStatement.append("	  BEGIN");
		createStatement.append("		UPDATE " + target.getName());
		createStatement.append("		  SET " + updates);
		createStatement.append("		  WHERE  " + identityCondition + ";");
		createStatement.append("	  EXCEPTION WHEN unique_violation THEN END;");
		createStatement.append("	END LOOP;");
		createStatement.append("  RETURN CONCAT('(', r." + Joiner.on(", ',', r.").join(identityColumnNames) + ", ')');");
		createStatement.append("END; $$ LANGUAGE 'plpgsql';");

		QueryBuilder dropStatement = new QueryBuilder();
		switch (stage) {
			case INITIAL:
				dropStatement.append("DROP FUNCTION " + functionName + "();");
				break;
			case CONSECUTIVE:
				List<String> parameterTypes = identityColumns.stream()
						.map(column -> column.getType().toString())
						.collect(Collectors.toList());

				dropStatement.append("DROP FUNCTION " + functionName + "(" + Joiner.on(",").join(parameterTypes) + ");");
				break;
		}

		return new MigratorFunction(functionName, identityColumnNames, createStatement.toString(), dropStatement.toString());
	}

	private static MigratorFunction createInsertMigrator(NullRecords nullRecords, RefLog refLog, Table source,
			Table target, Version from, Version to, long batchSize, Stage stage, Set<String> columns) {

		List<Column> identityColumns = source.getIdentityColumns();
		Map<String, String> functionParameterMapping = Maps.newHashMap();
		for (int i = 0; i < identityColumns.size(); i++) {
			functionParameterMapping.put(identityColumns.get(i).getName(), "q" + i);
		}

		List<String> identityColumnNames = identityColumns.stream()
				.map(column -> "\"" + column.getName() + "\"")
				.collect(Collectors.toList());

		List<String> functionParameters = identityColumns.stream()
				.map(column -> functionParameterMapping.get(column.getName()) + " " + column.getType().toString())
				.collect(Collectors.toList());

		Map<String, String> values = mapping.getColumnMappings().cellSet().stream()
				.filter(entry -> columns.contains(entry.getColumnKey()))
				.collect(Collectors.toMap(Cell::getColumnKey,
						entry -> "r.\"" + entry.getRowKey() + "\"",
						(u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
						Maps::newLinkedHashMap));

		for (ForeignKey foreignKey : target.getForeignKeys()) {
			List<String> foreignKeyColumns = foreignKey.getReferencingColumns();

			if (foreignKey.isNotNullable() && !values.keySet().containsAll(foreignKeyColumns)) {
				Table referredTable = foreignKey.getReferredTable();
				Identity identity = nullRecords.getIdentity(referredTable);
				LinkedHashMap<String, String> columnMapping = foreignKey.getColumnMapping();
				for (String columnName : foreignKeyColumns) {
					String referencedColumn = columnMapping.get(columnName);
					Column column = target.getColumn(columnName);

					String value = column.getDefaultValue();
					if (identity != null) {
						value = identity.getValue(referencedColumn).toString();
						if (column.getType().isRequireQuotes()) {
							value = "'" + value + "'";
						}
					}

					values.put(columnName, value);
				}
			}
		}

		String functionName = "migrator_" + RandomHasher.generateHash();

		QueryBuilder createStatement = new QueryBuilder();

		switch (stage) {
			case INITIAL:
				createStatement.append("CREATE FUNCTION " + functionName + "()");
				break;
			case CONSECUTIVE:
				createStatement.append(
						"CREATE FUNCTION " + functionName + "(" + Joiner.on(", ").join(functionParameters) + ")");
				break;
		}

		createStatement.append("  RETURNS text AS $$");
		createStatement.append("  DECLARE r record;");
		createStatement.append("  BEGIN");
		createStatement.append("	FOR r IN");
		createStatement.append("	  SELECT * FROM " + source.getName());

		if (stage != Stage.INITIAL) {
			createStatement.append("		WHERE");
			for (int i = 0; i < identityColumns.size(); i++) {
				if (i > 0) {
					createStatement.append("OR");
				}

				createStatement.append("(");

				for (int j = 0; j < i; j++) {
					String identityColumnName = identityColumns.get(j).getName();
					String value = functionParameterMapping.get(identityColumnName);
					createStatement.append(identityColumnName + " = " + value);
					createStatement.append("AND");
				}

				String identityColumnName = identityColumns.get(i).getName();
				String value = functionParameterMapping.get(identityColumnName);
				createStatement.append(identityColumnName + " > " + value);
				createStatement.append(")");
			}
		}

		createStatement.append("		ORDER BY " + Joiner.on(" ASC, ").join(identityColumnNames) + " ASC");
		createStatement.append("		LIMIT " + batchSize);
		createStatement.append("	LOOP");
		createStatement.append("	  BEGIN");
		createStatement.append("		INSERT INTO " + target.getName());
		createStatement.append("		  (" + values.keySet().stream().map(input -> "\"" + input + "\"").collect(Collectors.joining(", ")) + ")");
		createStatement.append("		  VALUES (" + Joiner.on(", ").join(values.values()) + ");");
		createStatement.append("	  EXCEPTION WHEN unique_violation THEN END;");
		createStatement.append("	END LOOP;");
		createStatement.append("  RETURN CONCAT('(', r." + Joiner.on(", ',', r.").join(identityColumnNames) + ", ')');");
		createStatement.append("END; $$ LANGUAGE 'plpgsql';");


		QueryBuilder dropStatement = new QueryBuilder();

		switch (stage) {
			case INITIAL:
				dropStatement.append("DROP FUNCTION " + functionName + "();");
				break;
			case CONSECUTIVE:
				List<String> parameterTypes = identityColumns.stream()
						.map(column -> column.getType().toString())
						.collect(Collectors.toList());

				dropStatement.append("DROP FUNCTION " + functionName + "(" + Joiner.on(",").join(parameterTypes) + ");");
				break;
		}

		return new MigratorFunction(functionName, identityColumnNames, createStatement.toString(), dropStatement.toString());
	}

}
