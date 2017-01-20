package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.LinkedHashMap;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

@Data
public class Function {

	private final String name;
	private final LinkedHashMap<String, DataType> parameters;
	private final DataType returnType;
	private final String body;
	private Catalog parent;

	public Function(String name, LinkedHashMap<String, DataType> parameters, DataType returnType, String body) {
		checkArgument(!isNullOrEmpty(name), "You must specify a 'name'.");
		checkArgument(parameters != null, "You must specify a 'parameters'.");
		checkArgument(returnType != null, "You must specify a 'returnType'.");
		checkArgument(!isNullOrEmpty(body), "You must specify a 'body'.");

		this.name = name;
		this.parameters = Maps.newLinkedHashMap();
		this.returnType = returnType;
		this.body = body;

		// Make defensive copy.
		this.parameters.putAll(parameters);
	}

	public List<DataType> getParameterTypes() {
		return Lists.newArrayList(parameters.values());
	}

	void setParent(Catalog parent) {
		if (parent == null && this.parent != null) {
			checkState(!this.parent.containsFunction(name, getParameterTypes()),
					"The function: " + name + " is still present in the catalog: " + this.parent.getName() + ".");
		}
		else if (parent != null && this.parent == null) {
			checkState(parent.containsFunction(name, getParameterTypes())
					&& this.equals(parent.containsFunction(name, getParameterTypes())),
					"The catalog: " + parent.getName() + " already contains a different function with the name: " + name);
		}

		this.parent = parent;
	}

	@Override
	public String toString() {
		return PrettyPrinter.prettyPrint(this);
	}

}
