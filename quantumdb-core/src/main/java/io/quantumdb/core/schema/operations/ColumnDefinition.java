package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import com.google.common.base.Strings;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType;
import lombok.Data;

@Data
public class ColumnDefinition {

	private final String name;
	private final ColumnType type;
	private final String defaultValueExpression;
	private final Column.Hint[] hints;

	private ColumnDefinition() {
		this.name = null;
		this.type = null;
		this.defaultValueExpression = null;
		this.hints = new Column.Hint[0];
	}

	ColumnDefinition(String name, ColumnType type, Column.Hint... hints) {
		this(name, type, null, hints);
	}

	ColumnDefinition(String name, ColumnType type, String defaultValueExpression, Column.Hint... hints) {
		checkArgument(!Strings.isNullOrEmpty(name), "You must specify a 'name'.");
		checkArgument(type != null, "You must specify a 'type'.");
		checkArgument(hints != null, "You may not specify 'hints' as NULL.");
		for (Column.Hint hint : hints) {
			checkArgument(hint != null, "You cannot add NULL as a hint.");
		}

		this.name = name;
		this.type = type;
		this.defaultValueExpression = defaultValueExpression;
		this.hints = hints;
	}

	public boolean isIdentity() {
		return containsHint(Column.Hint.IDENTITY);
	}

	public boolean isAutoIncrement() {
		return containsHint(Column.Hint.AUTO_INCREMENT);
	}

	public boolean isNotNull() {
		return containsHint(Column.Hint.NOT_NULL);
	}

	private boolean containsHint(Column.Hint needle) {
		return Arrays.stream(hints)
				.filter(hint -> hint == needle)
				.findFirst()
				.isPresent();
	}

	public Column createColumn() {
		return new Column(name, type, defaultValueExpression, hints);
	}

}
