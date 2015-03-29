package io.quantumdb.core.schema.definitions;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import lombok.Data;

@Data
public class ColumnType {

	public static enum Type {
		OID,
		UUID,
		CHAR,
		VARCHAR,
		TEXT,
		SMALLINT,
		INTEGER,
		BIGINT,
		DOUBLE,
		FLOAT,
		BOOLEAN,
		DATE,
		TIMESTAMP
	}

	@FunctionalInterface
	public interface ValueGenerator {
		Object generateValue();
	}

	@FunctionalInterface
	public interface ValueSetter {
		void setValue(PreparedStatement resultSet, int position, Object value) throws SQLException;
	}

	private final Type type;
	private final boolean requireQuotes;
	private final String notation;
	private final ValueGenerator valueGenerator;
	private final ValueSetter valueSetter;

	@Override
	public String toString() {
		return notation;
	}

}
