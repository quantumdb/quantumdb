package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Data
@EqualsAndHashCode(exclude = { "parent" })
@Setter(AccessLevel.NONE)
public class Column implements Copyable<Column> {

	public static enum Hint {
		NOT_NULL, AUTO_INCREMENT, IDENTITY;
	}

	private String name;
	private transient Table parent;

	private ColumnType type;
	private String defaultValueExpression;
	private final Set<Hint> hints;

	@Setter(AccessLevel.PROTECTED)
	@Getter(AccessLevel.PROTECTED)
	private transient ForeignKey outgoingForeignKey;

	@Getter(AccessLevel.PROTECTED)
	private transient List<ForeignKey> incomingForeignKeys;

	public Column(String name, ColumnType type, Hint... hints) {
		this(name, type, null, hints);
	}

	public Column(String name, ColumnType type, String defaultValueExpression, Hint... hints) {
		checkArgument(!Strings.isNullOrEmpty(name), "You must specify a 'name'.");
		checkArgument(type != null, "You must specify a 'type'.");
		checkArgument(hints != null, "You may not specify 'hints' as NULL.");
		for (Hint hint : hints) {
			checkArgument(hint != null, "You cannot add NULL as a hint.");
		}

		this.name = name;
		this.type = type;
		this.defaultValueExpression = defaultValueExpression;
		this.hints = Sets.newHashSet(hints);
		this.incomingForeignKeys = Lists.newArrayList();
	}

	void setParent(Table parent) {
		this.parent = parent;
	}

	void reference(String tableName, String columnName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");
		checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a 'columnName'.");
		checkState(parent != null, "This column must first be added to a table.");
		checkState(parent.getParent() != null, "The parent table must first be added to a catalog.");

		// TODO...
	}

	public void modifyType(ColumnType newColumnType) {
		this.type = newColumnType;
	}

	public void modifyDefaultValueExpression(String defaultValueExpression) {
		this.defaultValueExpression = defaultValueExpression;
	}

	public boolean isIdentity() {
		return hints.contains(Hint.IDENTITY);
	}

	public boolean isAutoIncrement() {
		return hints.contains(Hint.AUTO_INCREMENT);
	}

	public boolean isNotNull() {
		return hints.contains(Hint.NOT_NULL);
	}

	public void addHint(Hint hint) {
		hints.add(hint);
	}

	public void dropHint(Hint hint) {
		hints.remove(hint);
	}

	public Column rename(String newName) {
		checkArgument(!Strings.isNullOrEmpty(newName), "You must specify a 'name'.");
		if (parent != null) {
			checkArgument(!parent.containsColumn(newName),
					"Table: " + parent.getName() + " already contains column with name: " + newName);
		}

		this.name = newName;
		return this;
	}

	// TODO: This is PostgreSQL specific!
	public String getSequenceName() {
		String defaultValue = defaultValueExpression;
		String lowerCased = defaultValue.toLowerCase();

		String prefix = "nextval(";
		int index = lowerCased.indexOf(prefix);

		if (index == -1) {
			return null;
		}

		int start = index + prefix.length();
		int end = lowerCased.lastIndexOf(')');

		String substring = defaultValue.substring(start, end);

		start = substring.indexOf('\'');
		if (start == -1) {
			return substring;
		}

		end = substring.lastIndexOf('\'');
		return substring.substring(start + 1, end);
	}

	@Override
	public Column copy() {
		return new Column(name, type, defaultValueExpression, hints.stream().toArray(Hint[]::new));
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
