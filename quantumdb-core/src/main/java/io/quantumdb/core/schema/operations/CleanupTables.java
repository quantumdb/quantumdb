package io.quantumdb.core.schema.operations;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class CleanupTables implements SchemaOperation {

	CleanupTables() {
	}

}
