package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import lombok.Data;

@Data
public class ForeignKey {

	private final Table referencingTable;
	private final Table referredTable;
	private final String[] referencingColumns;
	private final String[] referredColumns;

	ForeignKey(Table referencingTable, String[] referencingColumns, Table referredTable, String[] referredColumns) {
		checkArgument(referredColumns.length == referencingColumns.length,
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
		this.referencingColumns = referencingColumns;
		this.referredColumns = referredColumns;
	}

	public String getReferredTableName() {
		return referredTable.getName();
	}

	public String getReferencingTableName() {
		return referencingTable.getName();
	}

	public String[] getReferredColumns() {
		return referredColumns;
	}

	public String[] getReferencingColumns() {
		return referencingColumns;
	}

	public void drop() {
		Arrays.stream(referencingColumns)
				.forEach(column -> referencingTable.getColumn(column).setOutgoingForeignKey(null));

		Arrays.stream(referredColumns)
				.forEach(column -> referredTable.getColumn(column).getIncomingForeignKeys().clear());

		referencingTable.dropForeignKey(this);
	}

}
