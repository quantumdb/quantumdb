package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class MergeTable implements SchemaOperation {

	private final String leftTableName;
	private final String rightTableName;
	private final String targetTableName;

	MergeTable(String leftTableName, String rightTableName, String targetTableName) {
		checkArgument(!Strings.isNullOrEmpty(leftTableName), "You must specify a 'leftTableName'.");
		checkArgument(!Strings.isNullOrEmpty(rightTableName), "You must specify a 'rightTableName'.");
		checkArgument(!Strings.isNullOrEmpty(targetTableName), "You must specify a 'targetTableName'.");

		this.leftTableName = leftTableName;
		this.rightTableName = rightTableName;
		this.targetTableName = targetTableName;
	}

}
