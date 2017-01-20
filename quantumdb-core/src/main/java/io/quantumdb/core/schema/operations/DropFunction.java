package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.DataType;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which drops an existing function.
 */
@Data
@Accessors(chain = true)
public class DropFunction implements SchemaOperation {

	private final String functionName;
	private final List<DataType> parameterTypes;

	DropFunction(String functionName) {
		checkArgument(!isNullOrEmpty(functionName), "You must specify a 'functionName'");

		this.functionName = functionName;
		this.parameterTypes = Lists.newArrayList();
	}

	public DropFunction withParameters(DataType... types) {
		checkArgument(types != null, "You must specify a 'type'.");
		Arrays.stream(types).forEachOrdered(parameterTypes::add);
		return this;
	}

}
