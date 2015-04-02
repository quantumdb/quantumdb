package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import lombok.Data;

@Data
public class ForeignKey {

	private final Table referencingTable;
	private final Table referredTable;
	private final ImmutableList<String> referencingColumns;
	private final ImmutableList<String> referredColumns;

	ForeignKey(Table referencingTable, List<String> referencingColumns, Table referredTable, List<String> referredColumns) {
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

		this.referencingTable = referencingTable;
		this.referredTable = referredTable;
		this.referencingColumns = ImmutableList.copyOf(referencingColumns);
		this.referredColumns = ImmutableList.copyOf(referredColumns);
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

	public Map<String, String> getColumns() {
		Map<String, String> columns = Maps.newLinkedHashMap();
		for (int i = 0; i < referredColumns.size(); i++) {
			String referencingColumnName = referencingColumns.get(i);
			String referredColumnName = referredColumns.get(i);
			columns.put(referencingColumnName, referredColumnName);
		}
		return columns;
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
