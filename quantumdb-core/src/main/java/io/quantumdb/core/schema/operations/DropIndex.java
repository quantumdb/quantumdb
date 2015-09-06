package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which adds a non-existent column to an already existing table.
 */
@Data
@Accessors(chain = true)
public class DropIndex implements SchemaOperation {

	private final String tableName;
	private final String[] columns;

	DropIndex(String tableName, String... columns) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'");
		checkArgument(columns.length > 0, "You must specify at least one 'column'");

		this.tableName = tableName;
		this.columns = columns;
	}
	
}
