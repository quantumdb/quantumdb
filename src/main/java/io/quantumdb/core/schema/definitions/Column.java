package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

@Data
@EqualsAndHashCode(exclude = { "parent" })
@Setter(AccessLevel.NONE)
public class Column implements Copyable<Column> {

	public static enum Hint {
		NOT_NULL, AUTO_INCREMENT, IDENTITY;
	}

	private String name;
	private Table parent;

	private final ColumnType type;
	private final String defaultValueExpression;
	private final Hint[] hints;

	public Column(String name, ColumnType type, Hint... hints) {
		this(name, type, null, hints);
	}

	public Column(String name, ColumnType type, String defaultValueExpression, Hint... hints) {
		checkArgument(!Strings.isNullOrEmpty(name), "You must specify a 'name'.");
		checkArgument(type != null, "You must specify a 'type'.");

		this.name = name;
		this.type = type;
		this.defaultValueExpression = defaultValueExpression;
		this.hints = hints;
	}

	void setParent(Table parent) {
		this.parent = parent;
	}

	public boolean isIdentity() {
		return containsHint(Hint.IDENTITY);
	}

	public boolean isAutoIncrement() {
		return containsHint(Hint.AUTO_INCREMENT);
	}

	public boolean isNotNull() {
		return containsHint(Hint.NOT_NULL);
	}

	private boolean containsHint(Hint needle) {
		return Arrays.stream(hints)
				.filter(hint -> hint == needle)
				.findFirst()
				.isPresent();
	}

	public void rename(String newName) {
		checkArgument(!Strings.isNullOrEmpty(newName), "You must specify a 'name'.");
		if (parent != null) {
			checkArgument(!parent.containsColumn(newName),
					"Table: " + parent.getName() + " already contains column with name: " + newName);
		}

		this.name = newName;
	}

	@Override
	public Column copy() {
		return new Column(name, type, defaultValueExpression, Arrays.copyOf(hints, hints.length));
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder().append("Column ").append("[" + name + "] " + type);
		if (!Strings.isNullOrEmpty(defaultValueExpression)) {
			builder.append(" default: '" + defaultValueExpression + "'");
		}
		for (Hint hint : hints) {
			builder.append(" " + hint.name());
		}
		return builder.toString();
	}

}
