package io.quantumdb.core.backends.postgresql;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import io.quantumdb.core.backends.postgresql.MigratorFunction.Stage;
import io.quantumdb.core.migration.utils.DataMapping;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.utils.RandomHasher;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RelationalMigratorFunction {

	static MigratorFunction create(Map<String, Identity> nullRecords, DataMapping mapping, long batchSize, Stage stage) {
		List<Column> identityColumns = mapping.getSourceTable().getIdentityColumns();
		Map<String, String> functionParameterMapping = Maps.newHashMap();
		for (int i = 0; i < identityColumns.size(); i++) {
			functionParameterMapping.put(identityColumns.get(i).getName(), "q" + i);
		}

		List<String> identityColumnNames = identityColumns.stream()
				.map(Column::getName)
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
		createStatement.append("    FOR r IN");
		createStatement.append("      SELECT * FROM " + mapping.getSourceTable().getName());

		if (stage != Stage.INITIAL) {
			createStatement.append("        WHERE");
			for (int i = 0; i < identityColumns.size(); i++) {
				if (i > 0) {
					createStatement.append("OR");
				}

				createStatement.append("(");

				for (int j = 0; j < i; j++) {
					String identityColumnName = identityColumns.get(j).getName();
					String value = functionParameterMapping.get(identityColumnName);
					Column targetColumn = mapping.getTargetTable().getColumn(identityColumnName);
					if (targetColumn.getType().isRequireQuotes()) {
						value = "'" + value + "'";
					}

					createStatement.append(identityColumnName + " = " + value);
					createStatement.append("AND");
				}

				String identityColumnName = identityColumns.get(i).getName();
				String value = functionParameterMapping.get(identityColumnName);
				Column targetColumn = mapping.getTargetTable().getColumn(identityColumnName);
				if (targetColumn.getType().isRequireQuotes()) {
					value = "'" + value + "'";
				}
				createStatement.append(identityColumnName + " > " + value);
				createStatement.append(")");
			}
		}

		Map<String, String> columnsToMigrate = mapping.getColumnMappings().entrySet().stream()
				.map(entry -> {
					String newColumnName = entry.getValue().getColumnName();
					Table newTable = mapping.getTargetTable();
					Column newColumn = newTable.getColumn(newColumnName);
					return new SimpleImmutableEntry<>(newColumn, entry.getKey());
				})
				.filter(entry -> {
					ForeignKey outgoingForeignKey = entry.getKey().getOutgoingForeignKey();
					if (outgoingForeignKey == null) {
						return false;
					}

					String referredTableName = outgoingForeignKey.getReferredTableName();
					return nullRecords.containsKey(referredTableName);
				})
				.collect(Collectors.toMap(entry -> entry.getKey().getName(), Entry::getValue));

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
				.map(column -> mapping.getColumnMappings().get(column.getName()).getColumnName() + " = r." + column.getName())
				.collect(Collectors.joining(" AND "));

		createStatement.append("        ORDER BY " + Joiner.on(" ASC, ").join(identityColumnNames) + " ASC");
		createStatement.append("        LIMIT " + batchSize);
		createStatement.append("    LOOP");
		createStatement.append("      BEGIN");
		createStatement.append("        UPDATE " + mapping.getTargetTable().getName());
		createStatement.append("          SET " + updates);
		createStatement.append("          WHERE  " + identityCondition + ";");
		createStatement.append("      EXCEPTION WHEN unique_violation THEN END;");
		createStatement.append("    END LOOP;");
		createStatement.append("  RETURN r." + Joiner.on(",r.").join(identityColumnNames) + ";");
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
