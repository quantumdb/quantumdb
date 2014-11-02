package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which drops an existing table from a catalog.
 */
@Data
@Accessors(chain = true)
public class DropTable implements SchemaOperation {

	private final String tableName;

	DropTable(String tableName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");

		this.tableName = tableName;
	}

}
