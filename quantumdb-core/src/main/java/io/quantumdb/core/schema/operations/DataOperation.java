package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;

@Data
public class DataOperation implements Operation {

	private final String query;

	DataOperation(String query) {
		checkArgument(!Strings.isNullOrEmpty(query), "You must specify a 'query'.");
		this.query = query;
	}

	@Override
	public Type getType() {
		return Type.DML;
	}

}
