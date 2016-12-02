package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;

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
@EqualsAndHashCode(exclude = { "parent", "outgoingForeignKey", "incomingForeignKeys", "sequence" })
@Setter(AccessLevel.NONE)
public class Column implements Copyable<Column> {

	public static enum Hint {
		NOT_NULL, AUTO_INCREMENT, IDENTITY;
	}

	private String name;
	private Table parent;

	private ColumnType type;
	private String defaultValue;
	private final Set<Hint> hints;

	@Setter(AccessLevel.PROTECTED)
	private ForeignKey outgoingForeignKey;

	@Setter(AccessLevel.PROTECTED)
	private Sequence sequence;

	@Getter(AccessLevel.PROTECTED)
	private final List<ForeignKey> incomingForeignKeys;

	public Column(String name, ColumnType type, Hint... hints) {
		this(name, type, null, null, hints);
	}

	public Column(String name, ColumnType type, String defaultValue, Hint... hints) {
		this(name, type, null, defaultValue, hints);
	}

	public Column(String name, ColumnType type, Sequence sequence, Hint... hints) {
		this(name, type, sequence, null, hints);
	}

	private Column(String name, ColumnType type, Sequence sequence, String defaultValueExpression, Hint... hints) {
		checkArgument(!Strings.isNullOrEmpty(name), "You must specify a 'name'.");
		checkArgument(type != null, "You must specify a 'type'.");
		checkArgument(hints != null, "You may not specify 'hints' as NULL.");
		for (Hint hint : hints) {
			checkArgument(hint != null, "You cannot add NULL as a hint.");
		}

		this.name = name;
		this.type = type;
		this.sequence = sequence;
		this.defaultValue = defaultValueExpression;
		this.hints = Sets.newHashSet(hints);
		this.incomingForeignKeys = Lists.newArrayList();
	}

	void setParent(Table parent) {
		this.parent = parent;
	}

	public void modifyType(ColumnType newColumnType) {
		this.type = newColumnType;
	}

	public void dropDefaultValue() {
		this.defaultValue = null;
		this.sequence = null;
		this.hints.remove(Hint.AUTO_INCREMENT);
	}

	public void modifyDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
		this.sequence = null;
		this.hints.remove(Hint.AUTO_INCREMENT);
	}

	public void modifyDefaultValue(Sequence sequence) {
		this.hints.add(Hint.AUTO_INCREMENT);
		this.sequence = sequence;
		this.defaultValue = null;
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

	@Override
	public Column copy() {
		return new Column(name, type, sequence, defaultValue, hints.stream().toArray(Hint[]::new));
	}

	@Override
	public String toString() {
		return PrettyPrinter.prettyPrint(this);
	}

}
