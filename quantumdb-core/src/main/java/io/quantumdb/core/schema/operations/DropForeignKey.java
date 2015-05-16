package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

@Data
@Setter(AccessLevel.NONE)
public class DropForeignKey implements SchemaOperation {

	private final String tableName;
	private final String[] referringColumnNames;

	DropForeignKey(String tableName, String... columns) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'table'.");
		checkArgument(columns != null && columns.length > 0, "You must specify at least one 'column'.");

		this.tableName = tableName;
		this.referringColumnNames = columns;
	}

}
