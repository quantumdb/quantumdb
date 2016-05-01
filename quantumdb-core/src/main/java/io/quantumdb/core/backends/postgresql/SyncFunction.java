package io.quantumdb.core.backends.postgresql;

import static io.quantumdb.core.utils.RandomHasher.generateHash;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.quantumdb.core.backends.postgresql.migrator.NullRecords;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.utils.QueryBuilder;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

@Data
public class SyncFunction {

	private final TableRef source;
	private final TableRef target;
	private final String functionName;
	private final String triggerName;
	private final RefLog refLog;
	private final Catalog catalog;
	private final NullRecords nullRecords;

	@Setter(AccessLevel.NONE)
	private ImmutableMap<String, String> insertExpressions;

	@Setter(AccessLevel.NONE)
	private ImmutableMap<String, String> updateExpressions;

	@Setter(AccessLevel.NONE)
	private ImmutableMap<String, String> updateIdentities;

	@Setter(AccessLevel.NONE)
	private ImmutableMap<String, String> updateIdentitiesForInserts;

	public SyncFunction(RefLog refLog, TableRef source, TableRef target, Catalog catalog, NullRecords nullRecords) {
		this(refLog, source, target, catalog, nullRecords, "sync_" + generateHash(), "trig_" + generateHash());
	}

	public SyncFunction(RefLog refLog, TableRef source, TableRef target, Catalog catalog, NullRecords nullRecords,
			String functionName, String triggerName) {

		this.refLog = refLog;
		this.nullRecords = nullRecords;
		this.source = source;
		this.target = target;
		this.catalog = catalog;
		this.functionName = functionName;
		this.triggerName = triggerName;
	}

	public void setColumnsToMigrate(Set<String> columnsToMigrate) {
		Table sourceTable = catalog.getTable(source.getTableId());
		Table targetTable = catalog.getTable(target.getTableId());
		Map<ColumnRef, ColumnRef> columnMapping = refLog.getColumnMapping(source, target);

		Map<String, String> mapping = columnMapping.entrySet().stream()
				.filter(entry -> {
					Column column = sourceTable.getColumn(entry.getKey().getName());
					return columnsToMigrate.contains(entry.getValue().getName()) || column.isIdentity();
				})
				.collect(Collectors.toMap(entry -> entry.getKey().getName(), entry -> entry.getValue().getName()));

		Map<String, String> expressions = mapping.entrySet().stream()
//				.collect(Collectors.toMap(entry -> "\"" + entry.getKey() + "\"",
				.collect(Collectors.toMap(entry -> "\"" + entry.getValue() + "\"",
						entry -> "NEW.\"" + entry.getKey() + "\"",
//						entry -> "NEW.\"" + entry.getValue() + "\"",
						(u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
						Maps::newLinkedHashMap));

		for (ForeignKey foreignKey : targetTable.getForeignKeys()) {
			List<String> foreignKeyColumns = foreignKey.getReferencingColumns();

			if (foreignKey.isNotNullable() && !mapping.keySet().containsAll(foreignKeyColumns)) {
				Table referredTable = foreignKey.getReferredTable();
				Identity identity = nullRecords.getIdentity(referredTable);
				LinkedHashMap<String, String> columnMappings = foreignKey.getColumnMapping();
				for (String columnName : foreignKeyColumns) {
					String referencedColumn = columnMappings.get(columnName);

					Column column = targetTable.getColumn(columnName);
					String value = column.getDefaultValue();
					if (identity != null) {
						value = identity.getValue(referencedColumn).toString();
						if (column.getType().isRequireQuotes()) {
							value = "'" + value + "'";
						}
					}

					expressions.put("\"" + columnName + "\"", value);
				}
			}
		}

		this.insertExpressions = ImmutableMap.copyOf(expressions);
		this.updateExpressions = ImmutableMap.copyOf(insertExpressions);

		this.updateIdentitiesForInserts = ImmutableMap.copyOf(targetTable.getIdentityColumns().stream()
				.collect(Collectors.toMap(column -> "\"" + column.getName() + "\"",
						column -> "NEW.\"" + reverseLookup(mapping, column.getName()) + "\"",
						(u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
						Maps::newLinkedHashMap)));

		this.updateIdentities = ImmutableMap.copyOf(targetTable.getIdentityColumns().stream()
				.collect(Collectors.toMap(column -> "\"" + column.getName() + "\"",
						column -> "OLD.\"" + reverseLookup(mapping, column.getName()) + "\"",
						(u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
						Maps::newLinkedHashMap)));
	}

	private String reverseLookup(Map<String, String> mapping, String value) {
		return mapping.entrySet().stream()
				.filter(entry -> entry.getValue().equals(value))
				.findFirst()
				.map(Entry::getKey)
				.get();
	}

	public QueryBuilder createFunctionStatement() {
		return new QueryBuilder()
				.append("CREATE OR REPLACE FUNCTION " + functionName + "()")
				.append("RETURNS TRIGGER AS $$")
				.append("BEGIN")
				.append("   IF TG_OP = 'INSERT' THEN")
				.append("	   LOOP")
				.append("		   UPDATE " + target.getTableId())
				.append("			   SET " + represent(updateIdentitiesForInserts, " = ", ", "))
				.append("			   WHERE " + represent(updateIdentitiesForInserts, " = ", " AND ") + ";")
				.append("		   IF found THEN EXIT; END IF;")
				.append("		   BEGIN")
				.append("			   INSERT INTO " + target.getTableId())
				.append("				   (" + represent(insertExpressions, Entry::getKey, ", ") + ") VALUES")
				.append("				   (" + represent(insertExpressions, Entry::getValue, ", ") + ");")
				.append("			   EXIT;")
				.append("		   EXCEPTION WHEN unique_violation THEN")
				.append("		   END;")
				.append("	   END LOOP;")
				.append("   ELSIF TG_OP = 'UPDATE' THEN")
				.append("	   LOOP")
				.append("		   UPDATE " + target.getTableId())
				.append("			   SET " + represent(updateExpressions, " = ", ", "))
				.append("			   WHERE " + represent(updateIdentities, " = ", " AND ") + ";")
				.append("		   IF found THEN EXIT; END IF;")
				.append("		   BEGIN")
				.append("			   INSERT INTO " + target.getTableId())
				.append("				   (" + represent(insertExpressions, Entry::getKey, ", ") + ") VALUES")
				.append("				   (" + represent(insertExpressions, Entry::getValue, ", ") + ");")
				.append("			   EXIT;")
				.append("		   EXCEPTION WHEN unique_violation THEN")
				.append("		   END;")
				.append("	   END LOOP;")
				.append("   ELSIF TG_OP = 'DELETE' THEN")
				.append("	   DELETE FROM " + target.getTableId())
				.append("		   WHERE " + represent(updateIdentities, " = ", " AND ") + ";")
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
				.append("ON " + source.getTableId())
				.append("FOR EACH ROW")
				.append("WHEN (pg_trigger_depth() = 0)")
				.append("EXECUTE PROCEDURE " + functionName + "();");
	}

}
