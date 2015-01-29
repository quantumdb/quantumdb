package io.quantumdb.core.backends.postgresql;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.migration.utils.DataMapping;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.utils.RandomHasher;
import lombok.Data;

@Data
public class MigratorFunction {

	static MigratorFunction create(DataMapping mapping, long batchSize, boolean initial) {
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

		List<String> targetColumnNames = mapping.getColumnMappings().values().stream()
				.map(DataMapping.ColumnMapping::getColumnName)
				.collect(Collectors.toList());

		List<String> sourceColumnNames = Lists.newArrayList(mapping.getColumnMappings().keySet());

		String functionName = "migrator_" + RandomHasher.generateHash();

		QueryBuilder createStatement = new QueryBuilder();

		if (initial) {
			createStatement.append("CREATE FUNCTION " + functionName + "()");
		}
		else {
			createStatement.append("CREATE FUNCTION " + functionName + "(" + Joiner.on(", ").join(functionParameters) + ")");
		}

		createStatement.append("  RETURNS text AS $$");
		createStatement.append("  DECLARE r record;");
		createStatement.append("  BEGIN");
		createStatement.append("    FOR r IN");
		createStatement.append("      SELECT * FROM " + mapping.getSourceTable().getName());

		if (!initial) {
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

		createStatement.append("        ORDER BY " + Joiner.on(" ASC, ").join(identityColumnNames) + " ASC");
		createStatement.append("        LIMIT " + batchSize);
		createStatement.append("    LOOP");
		createStatement.append("      BEGIN");
		createStatement.append("        INSERT INTO " + mapping.getTargetTable().getName());
		createStatement.append("          (" + Joiner.on(", ").join(targetColumnNames) + ")");
		createStatement.append("          VALUES (r." + Joiner.on(", r.").join(sourceColumnNames) + ");");
		createStatement.append("      EXCEPTION WHEN unique_violation THEN END;");
		createStatement.append("    END LOOP;");
		createStatement.append("  RETURN r." + Joiner.on(",r.").join(identityColumnNames) + ";");
		createStatement.append("END; $$ LANGUAGE 'plpgsql';");


		QueryBuilder dropStatement = new QueryBuilder();
		if (initial) {
			dropStatement.append("DROP FUNCTION " + functionName + "();");
		}
		else {
			List<String> parameterTypes = identityColumns.stream()
					.map(column -> column.getType().toString())
					.collect(Collectors.toList());

			dropStatement.append("DROP FUNCTION " + functionName + "(" + Joiner.on(",").join(parameterTypes) + ");");
		}

		return new MigratorFunction(functionName, identityColumnNames, createStatement.toString(), dropStatement.toString());
	}

	private final String name;
	private final List<String> parameters;
	private final String createStatement;
	private final String dropStatement;

}
