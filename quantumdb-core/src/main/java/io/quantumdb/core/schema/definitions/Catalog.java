package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.quantumdb.core.versioning.Version;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Catalog implements Copyable<Catalog> {

	private final String name;
	private final Collection<Table> tables;

	public Catalog(String name) {
		checkArgument(!Strings.isNullOrEmpty(name), "You must specify a 'name'");

		this.name = name;
		this.tables = Sets.newHashSet();
	}

	public Catalog addTable(Table table) {
		checkArgument(table != null, "You must specify a 'table'.");
		checkArgument(!containsTable(table.getName()), "Catalog: '" + name + "' already contains table: '" + table.getName() + "'.");
		checkArgument(!table.getColumns().isEmpty(), "Table: '" + table.getName() + "' doesn't contain any columns.");
		checkArgument(!table.getIdentityColumns().isEmpty(), "Table: '" + table.getName() + "' has no identity columns.");

		tables.add(table);
		table.setParent(this);
		return this;
	}

	public boolean containsTable(String tableName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'");

		return tables.stream()
				.filter(t -> t.getName().equals(tableName))
				.findFirst()
				.isPresent();
	}

	public Table getTable(String tableName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'");

		return tables.stream()
				.filter(t -> t.getName().equals(tableName))
				.findFirst()
				.orElseThrow(() -> new IllegalStateException(
						"Catalog: " + name + " does not contain a table: " + tableName));
	}

	public Table removeTable(String tableName) {
		Table table = getTable(tableName);
		table.canBeDropped();

		tables.remove(table);
		table.setParent(null);
		table.dropOutgoingForeignKeys();

		return table;
	}

	public ImmutableSet<Table> getTables() {
		return ImmutableSet.copyOf(tables);
	}

	@Override
	public Catalog copy() {
		Catalog schema = new Catalog(name);
		for (Table table : tables) {
			schema.addTable(table.copy());
		}
		return schema;
	}

	@Override
	public String toString() {
		return new PrettyStringWriter()
				.append("Catalog [" + name + "] {\n")
				.modifyIndent(1)
				.append(tables.stream().map(table -> table.toString() ).collect(Collectors.joining(",\n")))
				.append("\n")
				.modifyIndent(-1)
				.append("}")
				.toString();
	}

	public Collection<String> getTablesReferencingTable(String tableName) {
		return tables.stream()
				.filter(table -> table.referencesTable(tableName))
				.map(Table::getName)
				.collect(Collectors.toSet());
	}

}
