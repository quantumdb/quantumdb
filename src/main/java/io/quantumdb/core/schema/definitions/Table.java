package io.quantumdb.core.schema.definitions;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

@Data
@EqualsAndHashCode(exclude = { "parent" })
@Setter(AccessLevel.NONE)
public class Table implements Copyable<Table> {

	private String name;
	private Catalog parent;

	private final List<Column> columns = Lists.newArrayList();

	public Table(String name) {
		this.name = name;
	}

	void setParent(Catalog parent) {
		this.parent = parent;
	}

	public Table addColumn(Column column) {
		if (containsColumn(column.getName())) {
			throw new IllegalArgumentException("Table already contains a column with name: " + column.getName());
		}
		columns.add(column);
		column.setParent(this);
		return this;
	}

	public Table addColumns(Collection<Column> newColumns) {
		newColumns.forEach(this::addColumn);
		return this;
	}

	public Column getColumn(String columnName) {
		return columns.stream()
				.filter(c -> c.getName().equals(columnName))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						"Table: " + name + " does not contain column: " + columnName));
	}

	public List<Column> getIdentityColumns() {
		return getColumns().stream()
				.filter(Column::isIdentity)
				.collect(Collectors.toList());
	}

	public boolean containsColumn(String columnName) {
		return columns.stream()
				.filter(c -> c.getName().equals(columnName))
				.findFirst()
				.isPresent();
	}

	public void removeColumn(String columnName) {
		Column column = getColumn(columnName);
		column.setParent(null);
		columns.remove(column);
	}

	public ImmutableList<Column> getColumns() {
		return ImmutableList.copyOf(columns);
	}

	public void rename(String newName) {
		if (parent.containsTable(newName)) {
			throw new IllegalArgumentException("Catalog already contains table with name: " + newName);
		}
		this.name = newName;
	}

	@Override
	public Table copy() {
		Table copy = new Table(name);
		columns.stream().forEachOrdered(column -> copy.columns.add(column.copy()));
		return copy;
	}

	@Override
	public String toString() {
		return new PrettyStringWriter()
				.append("Table [" + name + "] {\n")
				.modifyIndent(1)
				.append(columns.stream().map(column -> column.toString()).collect(Collectors.joining(",\n")))
				.append("\n")
				.modifyIndent(-1)
				.append("}")
				.toString();
	}

}
