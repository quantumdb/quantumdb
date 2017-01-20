package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Catalog implements Copyable<Catalog> {

	private final String name;
	private final Collection<Table> tables;
	private final Collection<Sequence> sequences;
	private final Collection<Function> functions;

	public Catalog(String name) {
		checkArgument(!Strings.isNullOrEmpty(name), "You must specify a 'name'");

		this.name = name;
		this.tables = Sets.newTreeSet(Comparator.comparing(Table::getName));
		this.sequences = Sets.newTreeSet(Comparator.comparing(Sequence::getName));
		this.functions = Sets.newTreeSet(Comparator.comparing(Function::getName));
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

	public Catalog addSequence(Sequence sequence) {
		checkArgument(sequence != null, "You must specify a 'sequence'");

		sequences.add(sequence);
		sequence.setParent(this);
		return this;
	}

	public Sequence removeSequence(String sequenceName) {
		checkArgument(!Strings.isNullOrEmpty(sequenceName), "You must specify a 'sequenceName'");

		Sequence sequence = getSequence(sequenceName);
		sequences.remove(sequence);
		sequence.setParent(null);

		tables.stream()
				.flatMap(table -> table.getColumns().stream())
				.filter(column -> sequence.equals(column.getSequence()))
				.forEach(Column::dropDefaultValue);

		return sequence;
	}

	private Sequence getSequence(String sequenceName) {
		checkArgument(!Strings.isNullOrEmpty(sequenceName), "You must specify a 'sequenceName'");

		return sequences.stream()
				.filter(sequence -> sequence.getName().equals(sequenceName))
				.findFirst()
				.orElseThrow(() -> new IllegalStateException(
						"Catalog: " + name + " does not contain a sequence: " + sequenceName));
	}

	public Catalog addFunction(Function function) {
		checkArgument(function != null, "You must specify a 'function'.");
		checkState(!containsFunction(function.getName(), function.getParameterTypes()),
				"Table already has a function with name: " + function.getName());

		functions.add(function);
		function.setParent(this);
		return this;
	}

	public boolean containsFunction(String functionName, List<DataType> parameterTypes) {
		checkArgument(!isNullOrEmpty(functionName), "You must specify a 'functionName'.");
		checkArgument(parameterTypes != null, "You must specify a 'parameterTypes'.");

		return functions.stream()
				.anyMatch(function -> function.getName().equals(functionName)
						&& function.getParameterTypes().equals(parameterTypes));
	}

	public Function removeFunction(String functionName, List<DataType> parameterTypes) {
		checkArgument(!isNullOrEmpty(functionName), "You must specify a 'functionName'.");
		checkArgument(parameterTypes != null, "You must specify a 'parameterTypes'.");

		Function function = getFunction(functionName, parameterTypes);
		function.setParent(null);
		functions.remove(function);
		return function;
	}

	public Function getFunction(String functionName, List<DataType> parameterTypes) {
		checkArgument(!isNullOrEmpty(functionName), "You must specify a 'functionName'.");
		checkArgument(parameterTypes != null, "You must specify a 'parameterTypes'.");

		return functions.stream()
				.filter(function -> function.getName().equals(functionName)
						&& function.getParameterTypes().equals(parameterTypes))
				.findFirst()
				.orElse(null);
	}

	public ImmutableSet<Function> getFunctions() {
		return ImmutableSet.copyOf(functions);
	}

	public ImmutableSet<Table> getTables() {
		return ImmutableSet.copyOf(tables);
	}

	public ImmutableSet<ForeignKey> getForeignKeys() {
		return ImmutableSet.copyOf(tables.stream()
				.flatMap(table -> table.getForeignKeys().stream())
				.collect(Collectors.toSet()));
	}

	public ImmutableSet<Index> getIndexes() {
		return ImmutableSet.copyOf(tables.stream()
				.flatMap(table -> table.getIndexes().stream())
				.collect(Collectors.toSet()));
	}

	@Override
	public Catalog copy() {
		Catalog schema = new Catalog(name);
		for (Table table : tables) {
			schema.addTable(table.copy());
		}
		for (ForeignKey foreignKey : getForeignKeys()) {
			Table source = schema.getTable(foreignKey.getReferencingTableName());
			Table target = schema.getTable(foreignKey.getReferredTableName());
			source.addForeignKey(foreignKey.getReferencingColumns())
					.named(foreignKey.getForeignKeyName())
					.onDelete(foreignKey.getOnDelete())
					.onUpdate(foreignKey.getOnUpdate())
					.referencing(target, foreignKey.getReferredColumns());
		}
		for (Sequence sequence : sequences) {
			schema.addSequence(sequence.copy());
		}
		return schema;
	}

	@Override
	public String toString() {
		return PrettyPrinter.prettyPrint(this);
	}

	public Set<String> getTablesReferencingTable(String tableName) {
		return tables.stream()
				.filter(table -> table.referencesTable(tableName))
				.map(Table::getName)
				.collect(Collectors.toSet());
	}

}
