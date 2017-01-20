package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Set;

import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

@Data
@EqualsAndHashCode(exclude = { "parent" })
@Setter(AccessLevel.NONE)
public class Trigger implements Copyable<Trigger> {

	public enum OnAction {
		INSERT, UPDATE, DELETE, TRUNCATE
	}

	public enum Timing {
		BEFORE, AFTER
	}

	public enum Orientation {
		ROW, STATEMENT
	}

	private String name;
	private Table parent;
	private final Set<OnAction> onActions;
	private Timing timing;
	private String condition;
	private Orientation orientation;
	private Function function;

	public Trigger(String name) {
		checkArgument(!isNullOrEmpty(name), "You must specify a 'name'.");

		this.name = name;
		this.onActions = Sets.newHashSet();
	}

	void setParent(Table parent) {
		if (parent == null && this.parent != null) {
			checkState(!this.parent.containsTrigger(name),
					"The trigger: " + name + " is still present on the table: " + this.parent.getName() + ".");
		}
		else if (parent != null && this.parent == null) {
			checkState(parent.containsTrigger(name) && this.equals(parent.containsTrigger(name)),
					"The table: " + parent.getName() + " already contains a different trigger with the name: " + name);
		}

		this.parent = parent;
	}

	public Trigger rename(String name) {
		checkArgument(!isNullOrEmpty(name), "You must specify a 'name'.");
		if (parent != null) {
			checkState(!parent.containsTrigger(name),
					"Table: " + parent.getName() + " already contains trigger with name: " + name);
		}

		this.name = name;
		return this;
	}

	public Trigger setOnAction(OnAction... onActions) {
		return setOnAction(Sets.newHashSet(onActions));
	}

	public Trigger setOnAction(Set<OnAction> onActions) {
		checkArgument(onActions != null && !onActions.isEmpty(), "You must specify at least one 'onAction'.");

		this.onActions.clear();
		onActions.forEach(this.onActions::add);

		return this;
	}

	@Override
	public Trigger copy() {
		Trigger copy = new Trigger(name);
		copy.setOnAction(onActions);
		copy.timing = timing;
		copy.condition = condition;
		copy.orientation = orientation;
		copy.function = function;
		return copy;
	}

	@Override
	public String toString() {
		return PrettyPrinter.prettyPrint(this);
	}

}
