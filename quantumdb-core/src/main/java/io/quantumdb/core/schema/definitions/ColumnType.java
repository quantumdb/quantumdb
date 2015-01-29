package io.quantumdb.core.schema.definitions;

import lombok.Data;

@Data
public class ColumnType {

	private final PostgresTypes.Type type;
//	private final Integer length;
//	private final Integer precision;
//	private final Boolean withTimezone;
	private final boolean requireQuotes;
	private final String notation;

	@Override
	public String toString() {
//		StringBuilder builder = new StringBuilder().append(type);
//
//		if (length != null) {
//			builder.append("(" + length);
//			if (precision != null) {
//				builder.append("," + precision);
//			}
//			builder.append(")");
//		}
//
//		return builder.toString();

		return notation;
	}

}
