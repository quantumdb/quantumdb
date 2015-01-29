package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which drops an existing column from an existing table.
 */
@Data
@Accessors(chain = true)
public class DropColumn implements SchemaOperation {

	private final String tableName;
	private final String columnName;

	DropColumn(String tableName, String columnName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");
		checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a 'columnName'.");

		this.tableName = tableName;
		this.columnName = columnName;
	}

}
