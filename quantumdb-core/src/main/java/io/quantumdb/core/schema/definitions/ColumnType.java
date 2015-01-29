package io.quantumdb.core.schema.definitions;

import io.quantumdb.core.backends.postgresql.PostgresTypes;
import lombok.Data;

@Data
public class ColumnType {

	// TODO: Move PostgreSQL types abstraction to postgresql package.
	private final PostgresTypes.Type type;
	
	private final boolean requireQuotes;
	private final String notation;

	@Override
	public String toString() {
		return notation;
	}

}
