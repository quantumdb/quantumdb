package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import lombok.Data;

@Data
public class ForeignKey {

	public static enum Action {
		CASCADE, RESTRICT, NO_ACTION, SET_DEFAULT, SET_NULL
	}

	private final String foreignKeyName;
	private final Table referencingTable;
	private final Table referredTable;
	private final ImmutableList<String> referencingColumns;
	private final ImmutableList<String> referredColumns;
	private Action onUpdate;
	private Action onDelete;

	ForeignKey(String foreignKeyName, Table referencingTable, List<String> referencingColumns, Table referredTable,
			List<String> referredColumns, Action onUpdate, Action onDelete) {

		checkArgument(!isNullOrEmpty(foreignKeyName), "You must specify a 'foreignKeyName'.");
		checkArgument(onUpdate != null, "You must specify an 'onUpdate' action.");
		checkArgument(onDelete != null, "You must specify an 'onDelete' action.");
		checkArgument(referredColumns.size() == referencingColumns.size(),
				"You must refer to as many columns as you are referring from.");

		for (String referencingColumn : referencingColumns) {
			checkArgument(referencingTable.containsColumn(referencingColumn),
					"The column: " + referencingColumn + " is not present in table: " +referencingTable.getName());
		}
		for (String referredColumn : referredColumns) {
			checkArgument(referredTable.containsColumn(referredColumn),
					"The column: " + referredColumn + " is not present in table: " +referredTable.getName());
		}

		this.foreignKeyName = foreignKeyName;
		this.referencingTable = referencingTable;
		this.referredTable = referredTable;
		this.referencingColumns = ImmutableList.copyOf(referencingColumns);
		this.referredColumns = ImmutableList.copyOf(referredColumns);
		this.onUpdate = onUpdate;
		this.onDelete = onDelete;
	}

	public String getReferredTableName() {
		return referredTable.getName();
	}

	public String getReferencingTableName() {
		return referencingTable.getName();
	}

	public ImmutableList<String> getReferredColumns() {
		return referredColumns;
	}

	public ImmutableList<String> getReferencingColumns() {
		return referencingColumns;
	}

	public LinkedHashMap<String, String> getColumnMapping() {
		LinkedHashMap<String, String> mapping = Maps.newLinkedHashMap();
		for (int i = 0; i < referencingColumns.size(); i++) {
			String referencingColumn = referencingColumns.get(i);
			String referredColumn = referredColumns.get(i);
			mapping.put(referencingColumn, referredColumn);
		}
		return mapping;
	}

	public Map<String, String> getColumns() {
		Map<String, String> columns = Maps.newLinkedHashMap();
		for (int i = 0; i < referredColumns.size(); i++) {
			String referencingColumnName = referencingColumns.get(i);
			String referredColumnName = referredColumns.get(i);
			columns.put(referencingColumnName, referredColumnName);
		}
		return columns;
	}

	public boolean isNotNullable() {
		return referencingColumns.stream()
				.map(referencingTable::getColumn)
				.anyMatch(Column::isNotNull);
	}

	public boolean isSelfReferencing() {
		return referencingTable.equals(referredTable);
	}

	public boolean isInheritanceRelation() {
		Set<String> identityColumns = getReferencingTable().getIdentityColumns().stream()
				.map(Column::getName)
				.collect(Collectors.toSet());

		return referencingColumns.stream()
				.anyMatch(identityColumns::contains);
	}

	public void drop() {
		referencingColumns.stream()
				.forEach(column -> referencingTable.getColumn(column).setOutgoingForeignKey(null));

		referredColumns.stream()
				.forEach(column -> referredTable.getColumn(column).getIncomingForeignKeys().clear());

		referencingTable.dropForeignKey(this);
	}

	public boolean containsNonNullableColumns() {
		return referencingColumns.stream()
				.map(referencingTable::getColumn)
				.filter(Column::isNotNull)
				.findFirst()
				.map(column -> true)
				.orElse(false);
	}

	@Override
	public String toString() {
		return PrettyPrinter.prettyPrint(this);
	}
}
