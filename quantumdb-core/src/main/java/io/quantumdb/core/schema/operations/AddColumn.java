package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which adds a non-existent column to an already existing table.
 */
@Data
@Accessors(chain = true)
public class AddColumn implements SchemaOperation {

	private final String tableName;
	private final ColumnDefinition columnDefinition;

	AddColumn(String tableName, String columnName, ColumnType type, String defaultExpression, Hint... hints) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'");
		checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a 'columnName'");
		checkArgument(type != null, "You must specify a 'type'");

		this.tableName = tableName;
		this.columnDefinition = new ColumnDefinition(columnName, type, defaultExpression, hints);
	}
	
}
