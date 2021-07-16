package io.quantumdb.core.planner;

import static io.quantumdb.core.planner.QueryUtils.quoted;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.quantumdb.core.planner.MigratorFunction.Stage;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
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

		List<Column> primaryKeyColumns = source.getPrimaryKeyColumns();
		Map<String, String> functionParameterMapping = Maps.newHashMap();
		for (int i = 0; i < primaryKeyColumns.size(); i++) {
			functionParameterMapping.put(primaryKeyColumns.get(i).getName(), "q" + i);
		}

		List<String> primaryKeyColumnNames = primaryKeyColumns.stream()
				.map(column -> quoted(column.getName()))
				.collect(Collectors.toList());

		List<String> functionParameters = primaryKeyColumns.stream()
				.map(column -> functionParameterMapping.get(column.getName()) + " " + column.getType().toString())
				.collect(Collectors.toList());

		String functionName = "migrator_" + RandomHasher.generateHash();

		QueryBuilder createStatement = new QueryBuilder();

		switch (stage) {
			case INITIAL:
				createStatement.append("CREATE FUNCTION " + quoted(functionName) + "()");
				break;
			case CONSECUTIVE:
				createStatement.append(
						"CREATE FUNCTION " + quoted(functionName) + "(" + Joiner.on(", ").join(functionParameters) + ")");
				break;
		}

		createStatement.append("  RETURNS text AS $$");
		createStatement.append("  DECLARE r record;");
		createStatement.append("  BEGIN");
		createStatement.append("	FOR r IN");
		createStatement.append("	  SELECT * FROM " + quoted(source.getName()));

		if (stage != Stage.INITIAL) {
			createStatement.append("		WHERE");
			for (int i = 0; i < primaryKeyColumns.size(); i++) {
				if (i > 0) {
					createStatement.append("OR");
				}

				createStatement.append("(");

				for (int j = 0; j < i; j++) {
					String primaryKeyColumnName = primaryKeyColumns.get(j).getName();
					String value = functionParameterMapping.get(primaryKeyColumnName);
					createStatement.append(quoted(primaryKeyColumnName) + " = " + value);
					createStatement.append("AND");
				}

				String primaryKeyColumnName = primaryKeyColumns.get(i).getName();
				String value = functionParameterMapping.get(primaryKeyColumnName);
				createStatement.append(quoted(primaryKeyColumnName) + " > " + value);
				createStatement.append(")");
			}
		}

		Multimap<TableRef, TableRef> tableMapping = refLog.getTableMapping(from, to);

		TableRef sourceRef = tableMapping.keySet().stream()
				.filter(tableRef -> tableRef.getRefId().equals(source.getName()))
				.findFirst().get();

		TableRef targetRef = tableMapping.get(sourceRef).stream()
				.filter(tableRef -> tableRef.getRefId().equals(target.getName()))
				.findFirst().get();

		Map<ColumnRef, ColumnRef> columnMapping = refLog.getColumnMapping(sourceRef, targetRef);

		Map<String, String> columnsToMigrate = columnMapping.entrySet().stream()
				.filter(entry -> columnsToBeMigrated.contains(entry.getValue().getName()))
				.map(entry -> {
					String newColumnName = entry.getValue().getName();
					Column newColumn = target.getColumn(newColumnName);
					return new SimpleImmutableEntry<>(newColumn, entry.getKey().getName());
				})
				.collect(Collectors.toMap(entry -> entry.getKey().getName(),
						SimpleImmutableEntry::getValue));

		if (columnsToMigrate.isEmpty()) {
			return null;
		}

		String updates = columnsToMigrate.keySet().stream()
				.map(columnName -> {
					String oldColumnName = columnsToMigrate.get(columnName);
					return quoted(columnName) + " = r." + quoted(oldColumnName);
				})
				.collect(Collectors.joining(", "));

		String primaryKeyCondition = source.getPrimaryKeyColumns().stream()
				.map(column -> {
					String mappedColumnName = columnMapping.entrySet().stream()
							.filter(entry -> entry.getKey().getName().equals(column.getName()))
							.map(entry -> entry.getValue().getName())
							.findFirst().get();

					return quoted(mappedColumnName) + " = r." + quoted(column.getName());
				})
				.collect(Collectors.joining(" AND "));

		createStatement.append("		ORDER BY " + primaryKeyColumnNames.stream().map(value -> quoted(value) + " ASC").collect(Collectors.joining(", ")));
		createStatement.append("		LIMIT " + batchSize);
		createStatement.append("	LOOP");
		createStatement.append("	  BEGIN");
		createStatement.append("		UPDATE " + quoted(target.getName()));
		createStatement.append("		  SET " + updates);
		createStatement.append("		  WHERE  " + primaryKeyCondition + ";");
		createStatement.append("	  EXCEPTION WHEN unique_violation THEN END;");
		createStatement.append("	END LOOP;");
		createStatement.append("  RETURN CONCAT('(', " + primaryKeyColumnNames.stream().map(value -> "r." + quoted(value)).collect(Collectors.joining(",',', ")) + ", ')');");
		createStatement.append("END; $$ LANGUAGE 'plpgsql';");

		List<String> parameterTypes = primaryKeyColumns.stream()
				.map(column -> column.getType().getType().toString())
				.collect(Collectors.toList());

		QueryBuilder dropStatement = new QueryBuilder();
		switch (stage) {
			case INITIAL:
				dropStatement.append("DROP FUNCTION " + quoted(functionName) + "();");
				break;
			case CONSECUTIVE:
				dropStatement.append("DROP FUNCTION " + quoted(functionName) + "(" + Joiner.on(",").join(parameterTypes) + ");");
				break;
		}

		LinkedHashMap<String, String> zippedPrimaryKeys = Maps.newLinkedHashMap();
		for (int i = 0; i < primaryKeyColumnNames.size(); i++) {
			zippedPrimaryKeys.put(primaryKeyColumnNames.get(i), parameterTypes.get(i));
		}

		return new MigratorFunction(functionName, zippedPrimaryKeys, createStatement.toString(), dropStatement.toString());
	}

	private static MigratorFunction createInsertMigrator(NullRecords nullRecords, RefLog refLog, Table source,
			Table target, Version from, Version to, long batchSize, Stage stage, Set<String> columns) {

		List<Column> primaryKeyColumns = source.getPrimaryKeyColumns();
		Map<String, String> functionParameterMapping = Maps.newHashMap();
		for (int i = 0; i < primaryKeyColumns.size(); i++) {
			functionParameterMapping.put(primaryKeyColumns.get(i).getName(), "q" + i);
		}

		List<String> primaryKeyColumnNames = primaryKeyColumns.stream()
				.map(column -> quoted(column.getName()))
				.collect(Collectors.toList());

		List<String> functionParameters = primaryKeyColumns.stream()
				.map(column -> functionParameterMapping.get(column.getName()) + " " + column.getType().toString())
				.collect(Collectors.toList());

		Multimap<TableRef, TableRef> tableMapping = refLog.getTableMapping(from, to);

		TableRef sourceRef = tableMapping.keySet().stream()
				.filter(tableRef -> tableRef.getRefId().equals(source.getName()))
				.findFirst().get();

		TableRef targetRef = tableMapping.get(sourceRef).stream()
				.filter(tableRef -> tableRef.getRefId().equals(target.getName()))
				.findFirst().get();

		Map<ColumnRef, ColumnRef> columnMapping = refLog.getColumnMapping(sourceRef, targetRef);

		Map<String, String> values = columnMapping.entrySet().stream()
				.filter(entry -> columns.contains(entry.getValue().getName()))
				.collect(Collectors.toMap(entry -> entry.getValue().getName(),
						entry -> "r." + quoted(entry.getKey().getName()),
						(u, v) -> {
							throw new IllegalStateException(String.format("Duplicate key %s", u));
						},
						Maps::newLinkedHashMap));

		for (ForeignKey foreignKey : target.getForeignKeys()) {
			List<String> foreignKeyColumns = foreignKey.getReferencingColumns();

			if (foreignKey.isNotNullable() && !values.keySet().containsAll(foreignKeyColumns)) {
				Table referredTable = foreignKey.getReferredTable();
				Identity identity = nullRecords.getIdentity(referredTable);
				LinkedHashMap<String, String> columnMappings = foreignKey.getColumnMapping();
				for (String columnName : foreignKeyColumns) {
					String referencedColumn = columnMappings.get(columnName);
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
				createStatement.append("CREATE FUNCTION " + quoted(functionName) + "()");
				break;
			case CONSECUTIVE:
				createStatement.append(
						"CREATE FUNCTION " + quoted(functionName) + "(" + Joiner.on(", ").join(functionParameters) + ")");
				break;
		}

		createStatement.append("  RETURNS text AS $$");
		createStatement.append("  DECLARE r record;");
		createStatement.append("  BEGIN");
		createStatement.append("	FOR r IN");
		createStatement.append("	  SELECT * FROM " + quoted(source.getName()));

		if (stage != Stage.INITIAL) {
			createStatement.append("		WHERE");
			for (int i = 0; i < primaryKeyColumns.size(); i++) {
				if (i > 0) {
					createStatement.append("OR");
				}

				createStatement.append("(");

				for (int j = 0; j < i; j++) {
					String primaryKeyColumnName = primaryKeyColumns.get(j).getName();
					String value = functionParameterMapping.get(primaryKeyColumnName);
					createStatement.append(quoted(primaryKeyColumnName) + " = " + value);
					createStatement.append("AND");
				}

				String primaryKeyColumnName = primaryKeyColumns.get(i).getName();
				String value = functionParameterMapping.get(primaryKeyColumnName);
				createStatement.append(quoted(primaryKeyColumnName) + " > " + value);
				createStatement.append(")");
			}
		}

		createStatement.append("		ORDER BY " + primaryKeyColumnNames.stream().map(value -> quoted(value) + " ASC").collect(Collectors.joining(", ")));
		createStatement.append("		LIMIT " + batchSize);
		createStatement.append("	LOOP");
		createStatement.append("	  BEGIN");
		createStatement.append("		INSERT INTO " + quoted(target.getName()));
		createStatement.append("		  (" + values.keySet().stream().map(QueryUtils::quoted).collect(Collectors.joining(", ")) + ")");
		createStatement.append("		  VALUES (" + Joiner.on(", ").join(values.values()) + ");");
		createStatement.append("	  EXCEPTION WHEN unique_violation THEN END;");
		createStatement.append("	END LOOP;");
		createStatement.append("  RETURN CONCAT('(', " + primaryKeyColumnNames.stream().map(value -> "r." + quoted(value)).collect(Collectors.joining(",',', ")) + ", ')');");
		createStatement.append("END; $$ LANGUAGE 'plpgsql';");

		List<String> parameterTypes = primaryKeyColumns.stream()
				.map(column -> column.getType().getType().toString())
				.collect(Collectors.toList());

		QueryBuilder dropStatement = new QueryBuilder();

		switch (stage) {
			case INITIAL:
				dropStatement.append("DROP FUNCTION " + quoted(functionName) + "();");
				break;
			case CONSECUTIVE:
				dropStatement.append("DROP FUNCTION " + quoted(functionName) + "(" + Joiner.on(",").join(parameterTypes) + ");");
				break;
		}

		LinkedHashMap<String, String> zippedPrimaryKeys = Maps.newLinkedHashMap();
		for (int i = 0; i < primaryKeyColumnNames.size(); i++) {
			zippedPrimaryKeys.put(primaryKeyColumnNames.get(i), parameterTypes.get(i));
		}

		return new MigratorFunction(functionName, zippedPrimaryKeys, createStatement.toString(), dropStatement.toString());
	}

}
