package io.quantumdb.core.backends.postgresql.planner;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Table;
import lombok.Data;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

@Data
public class Operation {

	public static enum Type {
		ADD_NULL, DROP_NULL, COPY
	}

	private final Set<Table> tables;
	private final LinkedHashSet<String> columns;
	private final Type type;

	public Operation(Table table, LinkedHashSet<String> columns, Type type) {
		this(Sets.newHashSet(table), columns, type);
	}

	public Operation(Set<Table> tables, Type type) {
		this(tables, Sets.newLinkedHashSet(), type);
	}

	private Operation(Set<Table> tables, LinkedHashSet<String> columns, Type type) {
		this.tables = tables;
		this.columns = columns;
		this.type = type;
	}

	public ImmutableSet<Table> getTables() {
		return ImmutableSet.copyOf(tables);
	}

	public void addTable(Table table) {
		tables.add(table);
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder()
				.append(getTableNames())
				.append(columns)
				.append(type)
				.toHashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Operation) {
			Operation op = (Operation) other;
			return new EqualsBuilder()
					.append(getTableNames(), op.getTableNames())
					.append(columns, op.columns)
					.append(type, op.type)
					.isEquals();
		}
		return false;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(type.name());
		builder.append(" ");
		builder.append(getTableNames());
		if (columns != null && !columns.isEmpty()) {
			builder.append(" " + columns);
		}
		return builder.toString();
	}

	private Set<String> getTableNames() {
		return tables.stream()
				.map(Table::getName)
				.collect(Collectors.toSet());
	}

}
