package io.quantumdb.core.schema.operations;

public interface SchemaOperation extends Operation {

	default Type getType() {
		return Type.DDL;
	}

}
