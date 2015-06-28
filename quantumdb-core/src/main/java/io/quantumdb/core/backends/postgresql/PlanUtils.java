package io.quantumdb.core.backends.postgresql;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import io.quantumdb.core.backends.postgresql.planner.Operation;
import io.quantumdb.core.backends.postgresql.planner.Plan;
import io.quantumdb.core.backends.postgresql.planner.Step;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.ColumnType.Type;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.TableMapping;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PlanUtils {

	public static void graph(Catalog catalog) {
		System.out.println("digraph " + catalog.getName() + " {");
		System.out.println("\trankdir=\"LR\";\n");

		for (Table table : catalog.getTables()) {
			for (ForeignKey foreignKey : table.getForeignKeys()) {
				String other = foreignKey.getReferredTable().getName();
				if (foreignKey.isInheritanceRelation()) {
					System.out.println("\t" + table.getName() + " -> " + other + " [color=black];");
				}
				else if (foreignKey.isNotNullable()) {
					System.out.println("\t" + table.getName() + " -> " + other + " [color=red];");
				}
				else {
					System.out.println("\t" + table.getName() + " -> " + other + " [color=green];");
				}
			}
		}

		System.out.println("}");
	}

	public static void graph(Plan plan, TableMapping mapping) {
		System.out.println("digraph plan {");
//		System.out.println("\trankdir=\"LR\";\n");

		List<Step> steps = plan.getSteps();
		for (int i = 0; i < steps.size(); i++) {
			Step step = steps.get(i);
			Operation operation = step.getOperation();
			Operation.Type type = operation.getType();
			Set<String> columns = operation.getColumns();
			Set<String> tableNames = operation.getTables().stream()
					.map(Table::getName)
					.map(name -> mapping.getTableName(name).orElse("Unknown"))
					.collect(Collectors.toSet());

			System.out.println("\t" + i + "[label=\"" + type + " [" + Joiner.on(",").join(tableNames) + "] [" + Joiner.on(",").join(columns) + "]\"];");
		}
		System.out.println();
		for (int i = 0; i < steps.size(); i++) {
			Step step = steps.get(i);
			for (Step dependsOn : step.getDependencies()) {
				int index = steps.indexOf(dependsOn);
				System.out.println("\t" + i + " -> " + index);
			}
		}

		System.out.println("}");
	}

	public static void create(Catalog catalog) {
		System.out.println("Catalog catalog = new Catalog(\"" + catalog.getName() + "\");");
		System.out.println();

		for (Table table : catalog.getTables()) {
			System.out.println("Table " + normalize(table.getName()) + " = new Table(\"" + table.getName() + "\")");

			List<Column> columns = table.getColumns();
			for (int i = 0; i < columns.size(); i++) {
				Column column = columns.get(i);

				String defaultValue = "";
				if (column.getDefaultValue() != null) {
					defaultValue = ", \"'" + column.getDefaultValue() + "'\"";
				}

				String hints = "";
				if (!column.getHints().isEmpty()) {
					hints = ", " + Joiner.on(", ").join(column.getHints());
				}

				System.out.print("\t\t.addColumn(new Column(\"" + column.getName() + "\", " +
						toString(column.getType()) + defaultValue + hints + "))");

				if (i == columns.size() - 1) {
					System.out.println(";");
				}
				else {
					System.out.println("");
				}
			}

			System.out.println();
		}

		for (Table table : catalog.getTables()) {
			System.out.println("catalog.addTable(" + normalize(table.getName()) + ");");
		}

		System.out.println();
		for (Table table : catalog.getTables()) {
			for (ForeignKey foreignKey : table.getForeignKeys()) {
				String fromTableName = normalize(foreignKey.getReferencingTableName());
				String toTableName = normalize(foreignKey.getReferredTableName());

				String fromColumns = "\"" + Joiner.on("\", \"").join(foreignKey.getReferencingColumns()) + "\"";
				String toColumns = "\"" + Joiner.on("\", \"").join(foreignKey.getReferredColumns()) + "\"";

				System.out.println(fromTableName + ".addForeignKey(" + fromColumns + ").referencing(" + toTableName + ", " + toColumns + ");");
			}
		}
	}

	private static String normalize(String name) {
		boolean capitalize = false;
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < name.length(); i++) {
			char c = name.charAt(i);
			if (c == '_') {
				capitalize = true;
			}
			else {
				if (capitalize) {
					builder.append(Character.toString(c).toUpperCase());
				}
				else {
					builder.append(c);
				}
				capitalize = false;
			}
		}
		return builder.toString();
	}

	private static String toString(ColumnType columnType) {
		Type type = columnType.getType();
		switch (type) {
			case BIGINT: return "bigint()";
			case INTEGER: return "integer()";
			case SMALLINT: return "smallint()";
			case OID: return "oid()";
			case UUID: return "uuid()";
			case TEXT: return "text()";
			case BOOLEAN: return "bool()";
			case DATE: return "date()";
			case DOUBLE: return "doubles()";
			case FLOAT: return "floats()";
			case VARCHAR: return "varchar(255)";
			case CHAR: return "chars(255)";
			case TIMESTAMP: return "timestamp(true)";
		}
		throw new IllegalStateException("Unsupported type: " + type);
	}

}
