package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class RenameTable implements SchemaOperation {

	private final String tableName;
	private final String newTableName;

	RenameTable(String tableName, String newTableName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");
		checkArgument(!Strings.isNullOrEmpty(newTableName), "You must specify a 'newTableName'.");

		this.tableName = tableName;
		this.newTableName = newTableName;
	}

}
