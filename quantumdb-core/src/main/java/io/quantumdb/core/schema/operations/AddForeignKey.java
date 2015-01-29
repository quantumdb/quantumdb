package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

@Data
@Setter(AccessLevel.NONE)
public class AddForeignKey implements SchemaOperation {

	private final String referringTableName;
	private final String[] referringColumnNames;

	private String referencedTableName;
	private String[] referencedColumnNames;

	AddForeignKey(String table, String... columns) {
		checkArgument(!Strings.isNullOrEmpty(table), "You must specify a 'table'.");
		checkArgument(columns != null && columns.length > 0, "You must specify at least one 'column'.");

		this.referringTableName = table;
		this.referringColumnNames = columns;
	}

	public AddForeignKey referencing(String table, String... columns) {
		checkArgument(!Strings.isNullOrEmpty(table), "You must specify a 'table'.");
		checkArgument(columns != null && columns.length > 0, "You must specify at least one 'column'.");
		checkArgument(columns.length == referringColumnNames.length, "The number of columns is mismatched.");

		this.referencedTableName = table;
		this.referencedColumnNames = columns;
		return this;
	}


}
