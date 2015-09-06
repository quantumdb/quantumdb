package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.ForeignKey.Action;
import io.quantumdb.core.utils.RandomHasher;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

@Data
@EqualsAndHashCode(exclude = { "parent", "foreignKeys" })
@Setter(AccessLevel.NONE)
public class Table implements Copyable<Table>, Comparable<Table> {

	public static class ForeignKeyBuilder {

		private String name;
		private final List<String> referringColumns;
		private final Table parentTable;
		private Action onUpdate;
		private Action onDelete;

		private ForeignKeyBuilder(Table parent, List<String> referringColumnNames) {
			this.name = "fk_" + RandomHasher.generateHash();
			this.parentTable = parent;
			this.referringColumns = referringColumnNames;
			this.onUpdate = Action.NO_ACTION;
			this.onDelete = Action.NO_ACTION;
		}

		public ForeignKeyBuilder onDelete(Action action) {
			this.onDelete = action;
			return this;
		}

		public ForeignKeyBuilder onUpdate(Action action) {
			this.onUpdate = action;
			return this;
		}

		public ForeignKeyBuilder named(String name) {
			this.name = name;
			return this;
		}

		public ForeignKey referencing(Table table, String... referredColumns) {
			return referencing(table, Lists.newArrayList(referredColumns));
		}

		public ForeignKey referencing(Table table, List<String> referredColumns) {
			ForeignKey constraint = new ForeignKey(name, parentTable, referringColumns, table, referredColumns,
					onUpdate, onDelete);

			referringColumns.forEach(column -> parentTable.getColumn(column).setOutgoingForeignKey(constraint));
			referredColumns.forEach(column -> table.getColumn(column).getIncomingForeignKeys().add(constraint));

			parentTable.foreignKeys.add(constraint);

			return constraint;
		}
	}

	private String name;
	private Catalog parent;

	private final LinkedHashSet<Column> columns = Sets.newLinkedHashSet();
	private final List<ForeignKey> foreignKeys = Lists.newArrayList();

	public Table(String name) {
		checkArgument(!Strings.isNullOrEmpty(name), "You must specify a 'name'.");
		this.name = name;
	}

	void setParent(Catalog parent) {
		if (parent == null && this.parent != null) {
			checkState(!this.parent.containsTable(name),
					"The table: " + name + " is still present in the catalog: " + this.parent.getName() + ".");
		}
		else if (parent != null && this.parent == null) {
			checkState(parent.containsTable(name) && this.equals(parent.getTable(name)),
					"The catalog: " + parent.getName() + " already contains a different table with the name: " + name);
		}

		this.parent = parent;
	}

	public Table addColumn(Column column) {
		checkArgument(column != null, "You must specify a 'column'.");
		checkState(!containsColumn(column.getName()), "Table already contains a column with name: " + column.getName());

		columns.add(column);
		column.setParent(this);
		return this;
	}

	public Table addColumns(Collection<Column> newColumns) {
		checkArgument(newColumns != null, "You must specify a 'newColumns'.");
		newColumns.forEach(this::addColumn);
		return this;
	}

	public Column getColumn(String columnName) {
		checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a 'columnName'.");

		return columns.stream()
				.filter(c -> c.getName().equals(columnName))
				.findFirst()
				.orElseThrow(() -> new IllegalStateException(
						"Table: " + name + " does not contain column: " + columnName));
	}

	public List<Column> getIdentityColumns() {
		return getColumns().stream()
				.filter(Column::isIdentity)
				.collect(Collectors.toList());
	}

	public boolean containsColumn(String columnName) {
		checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a 'name'.");

		return columns.stream()
				.filter(c -> c.getName().equals(columnName))
				.findFirst()
				.isPresent();
	}

	public Column removeColumn(String columnName) {
		checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a 'name'.");
		checkState(containsColumn(columnName), "You cannot remove a column which does not exist: " + columnName);

		Column column = getColumn(columnName);
		checkState(column.getIncomingForeignKeys().isEmpty(),
				"You cannot remove a column that is still referenced by foreign keys.");

		List<Column> identityColumns = getIdentityColumns();
		identityColumns.remove(column);
		checkState(!identityColumns.isEmpty(), "You drop the last remaining identity column of a table.");

		if (column.getOutgoingForeignKey() != null) {
			column.getOutgoingForeignKey().drop();
		}

		column.setParent(null);
		columns.remove(column);

		return column;
	}

	public ImmutableList<Column> getColumns() {
		return ImmutableList.copyOf(columns);
	}

	public ForeignKeyBuilder addForeignKey(String... referringColumns) {
		return addForeignKey(Lists.newArrayList(referringColumns));
	}

	public ForeignKeyBuilder addForeignKey(List<String> referringColumns) {
		return new ForeignKeyBuilder(this, referringColumns);
	}

	void dropForeignKey(ForeignKey constraint) {
		foreignKeys.remove(constraint);
	}

	public Table rename(String newName) {
		checkArgument(!Strings.isNullOrEmpty(newName), "You must specify a 'name'.");
		if (parent != null) {
			checkState(!parent.containsTable(newName),
					"Catalog: " + parent.getName() + " already contains table with name: " + newName);
		}

		this.name = newName;
		return this;
	}

	void canBeDropped() {
		for (Column column : columns) {
			checkState(column.getIncomingForeignKeys().isEmpty(), "The column: " + column.getName()
					+ " in table: " + name + " is still being referenced by " + column.getIncomingForeignKeys().size()
					+ " foreign key constraints.");
		}
	}

	void dropOutgoingForeignKeys() {
		columns.stream()
				.filter(column -> column.getOutgoingForeignKey() != null)
				.map(column -> column.getOutgoingForeignKey())
				.distinct()
				.forEach(ForeignKey::drop);
	}

	public boolean referencesTable(String tableName) {
		return foreignKeys.stream()
				.filter(foreignKey -> foreignKey.getReferredTableName().equals(tableName))
				.findAny()
				.isPresent();
	}

	public Set<String> enumerateReferencedByTables() {
		return parent.getTablesReferencingTable(getName());
	}

	@Override
	public Table copy() {
		Table copy = new Table(name);
		columns.stream().forEachOrdered(column -> copy.addColumn(column.copy()));
		return copy;
	}

	@Override
	public int compareTo(Table o) {
		return name.compareTo(o.name);
	}

	@Override
	public String toString() {
		return PrettyPrinter.prettyPrint(this);
	}

}
