package io.quantumdb.core.schema.operations;

public interface Operation {

	enum Type {
		DML, DDL
	}

	Type getType();

}