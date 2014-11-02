package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which copies an existing table to another not yet existing table.
 */
@Data
@Accessors(chain = true)
public class CopyTable implements SchemaOperation {

	private final String sourceTableName;
	private final String targetTableName;

	CopyTable(String sourceTableName, String targetTableName) {
		checkArgument(!Strings.isNullOrEmpty(sourceTableName), "You must specify 'sourceTableName'.");
		checkArgument(!Strings.isNullOrEmpty(targetTableName), "You must specify 'targetTableName'.");

		this.sourceTableName = sourceTableName;
		this.targetTableName = targetTableName;
	}

}
