package io.quantumdb.core.backends.postgresql;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table.Cell;
import io.quantumdb.core.backends.postgresql.migrator.NullRecords;
import io.quantumdb.core.migration.utils.DataMapping;
import io.quantumdb.core.migration.utils.DataMapping.Transformation;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.utils.RandomHasher;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

@Data
public class SyncFunction {

	private final String sourceTableId;
	private final String targetTableId;
	private final String functionName;
	private final String triggerName;
	private final DataMapping dataMapping;
	private final NullRecords nullRecords;

	@Setter(AccessLevel.NONE)
	private ImmutableMap<String, String> insertExpressions;

	@Setter(AccessLevel.NONE)
	private ImmutableMap<String, String> updateExpressions;

	@Setter(AccessLevel.NONE)
	private ImmutableMap<String, String> updateIdentities;

	public SyncFunction(DataMapping dataMapping, NullRecords nullRecords) {
		this(dataMapping, nullRecords, "sync_" + RandomHasher.generateHash(), "trig_" + RandomHasher.generateHash());
	}

	public SyncFunction(DataMapping dataMapping, NullRecords nullRecords, String functionName, String triggerName) {
		this.dataMapping = dataMapping;
		this.nullRecords = nullRecords;
		this.sourceTableId = dataMapping.getSourceTable().getName();
		this.targetTableId = dataMapping.getTargetTable().getName();
		this.functionName = functionName;
		this.triggerName = triggerName;
	}

	public void setColumnsToMigrate(Set<String> columnsToMigrate) {
		Table sourceTable = dataMapping.getSourceTable();
		Table targetTable = dataMapping.getTargetTable();

		List<Cell<String, String, Transformation>> entries = dataMapping.getColumnMappings().cellSet().stream()
				.filter(cell -> {
					Column column = sourceTable.getColumn(cell.getRowKey());
					return columnsToMigrate.contains(cell.getColumnKey()) || column.isIdentity();
				})
				.collect(Collectors.toList());

		Map<String, String> mapping = entries.stream()
				.collect(Collectors.toMap(Cell::getColumnKey, Cell::getRowKey,
						(u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
						Maps::newLinkedHashMap));

		Map<String, String> expressions = mapping.entrySet().stream()
				.collect(Collectors.toMap(entry -> "\"" + entry.getKey() + "\"",
						entry -> "NEW.\"" + entry.getValue() + "\"",
						(u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
						Maps::newLinkedHashMap));

		for (ForeignKey foreignKey : targetTable.getForeignKeys()) {
			List<String> foreignKeyColumns = foreignKey.getReferencingColumns();

			if (foreignKey.isNotNullable() && !mapping.keySet().containsAll(foreignKeyColumns)) {
				Table referredTable = foreignKey.getReferredTable();
				Identity identity = nullRecords.getIdentity(referredTable);
				LinkedHashMap<String, String> columnMapping = foreignKey.getColumnMapping();
				for (String columnName : foreignKeyColumns) {
					String referencedColumn = columnMapping.get(columnName);
					String value = identity.getValue(referencedColumn).toString();

					Column column = targetTable.getColumn(columnName);
					if (column.getType().isRequireQuotes()) {
						value = "'" + value + "'";
					}
					expressions.put("\"" + columnName + "\"", value);
				}
			}
		}

		this.insertExpressions = ImmutableMap.copyOf(expressions);
		this.updateExpressions = ImmutableMap.copyOf(insertExpressions);

		this.updateIdentities = ImmutableMap.copyOf(dataMapping.getTargetTable().getIdentityColumns().stream()
				.collect(Collectors.toMap(column -> "\"" + column.getName() + "\"",
						column -> "OLD.\"" + mapping.get(column.getName()) + "\"",
						(u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
						Maps::newLinkedHashMap)));
	}

	public QueryBuilder createFunctionStatement() {
		return new QueryBuilder()
				.append("CREATE OR REPLACE FUNCTION " + functionName + "()")
				.append("RETURNS TRIGGER AS $$")
				.append("BEGIN")
				.append("   IF TG_OP = 'INSERT' THEN")
				.append("       LOOP")
				.append("           UPDATE " + targetTableId)
				.append("               SET " + represent(updateExpressions, " = ", ", "))
				.append("               WHERE " + represent(updateIdentities, " = ", " AND ") + ";")
				.append("           IF found THEN EXIT; END IF;")
				.append("           BEGIN")
				.append("               INSERT INTO " + targetTableId)
				.append("                   (" + represent(insertExpressions, Entry::getKey, ", ") + ") VALUES")
				.append("                   (" + represent(insertExpressions, Entry::getValue, ", ") + ");")
				.append("               EXIT;")
				.append("           EXCEPTION WHEN unique_violation THEN")
				.append("           END;")
				.append("       END LOOP;")
				.append("   ELSIF TG_OP = 'UPDATE' THEN")
				.append("       LOOP")
				.append("           UPDATE " + targetTableId)
				.append("               SET " + represent(updateExpressions, " = ", ", "))
				.append("               WHERE " + represent(updateIdentities, " = ", " AND ") + ";")
				.append("           IF found THEN EXIT; END IF;")
				.append("           BEGIN")
				.append("               INSERT INTO " + targetTableId)
				.append("                   (" + represent(insertExpressions, Entry::getKey, ", ") + ") VALUES")
				.append("                   (" + represent(insertExpressions, Entry::getValue, ", ") + ");")
				.append("               EXIT;")
				.append("           EXCEPTION WHEN unique_violation THEN")
				.append("           END;")
				.append("       END LOOP;")
				.append("   ELSIF TG_OP = 'DELETE' THEN")
				.append("       DELETE FROM " + targetTableId)
				.append("           WHERE " + represent(updateIdentities, " = ", " AND ") + ";")
				.append("   END IF;")
				.append("   RETURN NEW;")
				.append("END;")
				.append("$$ LANGUAGE 'plpgsql';");
	}

	private String represent(Map<String, String> inputs, String innerJoin, String entryJoin) {
		return inputs.entrySet().stream()
				.map(entry -> entry.getKey() + innerJoin + entry.getValue())
				.collect(Collectors.joining(entryJoin));
	}

	private String represent(Map<String, String> inputs, Function<Entry<String, String>, String> selector, String join) {
		return inputs.entrySet().stream()
				.map(selector)
				.collect(Collectors.joining(join));
	}

	public QueryBuilder createTriggerStatement() {
		return new QueryBuilder()
				.append("CREATE TRIGGER " + triggerName)
				.append("AFTER INSERT OR UPDATE OR DELETE")
				.append("ON " + sourceTableId)
				.append("FOR EACH ROW")
				.append("WHEN (pg_trigger_depth() = 0)")
				.append("EXECUTE PROCEDURE " + functionName + "();");
	}

}
