package io.quantumdb.core.migration.operations;

import java.util.List;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.DataType;
import io.quantumdb.core.schema.definitions.Function;
import io.quantumdb.core.schema.operations.CreateFunction;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class CreateFunctionMigrator implements SchemaOperationMigrator<CreateFunction> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, CreateFunction operation) {
		List<DataType> parameterTypes = operation.getParameterTypes();
		if (operation.isOrReplace() && catalog.containsFunction(operation.getFunctionName(), parameterTypes)) {
			catalog.removeFunction(operation.getFunctionName(), parameterTypes);
		}
		catalog.addFunction(new Function(operation.getFunctionName(), operation.getParameters(),
				operation.getReturnType(), operation.getBody()));
	}

}
