package io.quantumdb.core.backends.postgresql;

import java.util.List;
import java.util.Map;
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
public class BaseMigratorFunction {

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

		List<String> targetColumnNames = mapping.getColumnMappings().values().stream()
				.map(DataMapping.ColumnMapping::getColumnName)
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

		String values = mapping.getColumnMappings().entrySet().stream()
				.map(entry -> {
					String newColumnName = entry.getValue().getColumnName();
					Table newTable = mapping.getTargetTable();
					Column newColumn = newTable.getColumn(newColumnName);

					ForeignKey outgoingForeignKey = newColumn.getOutgoingForeignKey();
					if (outgoingForeignKey != null) {
						String referredTableId = outgoingForeignKey.getReferredTable().getName();
						if (nullRecords.containsKey(referredTableId)) {
							Map<String, String> foreignKeyColumns = outgoingForeignKey.getColumns();
							String foreignColumnName = foreignKeyColumns.get(newColumnName);

							Identity identity = nullRecords.get(referredTableId);
							String value = identity.getValue(foreignColumnName).toString();

							if (newColumn.getType().isRequireQuotes()) {
								value = "'" + value + "'";
							}
							return value;
						}
					}

					return "r." + entry.getKey();
				})
				.collect(Collectors.joining(", "));

		createStatement.append("        ORDER BY " + Joiner.on(" ASC, ").join(identityColumnNames) + " ASC");
		createStatement.append("        LIMIT " + batchSize);
		createStatement.append("    LOOP");
		createStatement.append("      BEGIN");
		createStatement.append("        INSERT INTO " + mapping.getTargetTable().getName());
		createStatement.append("          (" + Joiner.on(", ").join(targetColumnNames) + ")");
		createStatement.append("          VALUES (" + values + ");");
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
