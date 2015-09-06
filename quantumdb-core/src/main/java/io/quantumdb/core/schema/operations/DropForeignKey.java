package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class DropForeignKey implements SchemaOperation {

	private final String tableName;
	private final String foreignKeyName;

	DropForeignKey(String tableName, String foreignKeyName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'table'.");
		checkArgument(!Strings.isNullOrEmpty(foreignKeyName), "You must specify a 'foreignKeyName'.");

		this.tableName = tableName;
		this.foreignKeyName = foreignKeyName;
	}

}
