package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.LinkedHashMap;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.quantumdb.core.schema.definitions.DataType;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which creates a new function.
 */
@Data
@Accessors(chain = true)
public class CreateFunction implements SchemaOperation {

	private final String functionName;
	private final LinkedHashMap<String, DataType> parameters;
	private boolean orReplace = false;
	private DataType returnType;
	private String body;

	CreateFunction(String functionName) {
		checkArgument(!isNullOrEmpty(functionName), "You must specify a 'functionName'");

		this.functionName = functionName;
		this.parameters = Maps.newLinkedHashMap();
	}

	public List<DataType> getParameterTypes() {
		return Lists.newArrayList(parameters.values());
	}

	public CreateFunction orReplace() {
		this.orReplace = true;
		return this;
	}

	public CreateFunction withParameter(String parameterName, DataType type) {
		checkArgument(!isNullOrEmpty(parameterName), "You must specify a 'parameterName'");
		checkArgument(type != null, "You must specify a 'type'.");
		this.parameters.put(parameterName, type);
		return this;
	}

	public CreateFunction returning(DataType returnType) {
		checkArgument(returnType != null, "You must specify a 'returnType'.");
		this.returnType = returnType;
		return this;
	}

	public CreateFunction withBody(String body) {
		checkArgument(!isNullOrEmpty(body), "You must specify a 'body'");
		this.body = body;
		return this;
	}

}
