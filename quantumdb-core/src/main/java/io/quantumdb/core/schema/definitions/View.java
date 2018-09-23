package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

@Data
@EqualsAndHashCode(exclude = { "parent" })
@Setter(AccessLevel.NONE)
public class View implements Copyable<View>, Comparable<View> {

	private String name;
	private Catalog parent;
	private boolean temporary;
	private boolean recursive;
	private String query;

	public View(String name, String query) {
		checkArgument(!Strings.isNullOrEmpty(name), "You must specify a 'name'.");
		checkArgument(!Strings.isNullOrEmpty(query), "You must specify a 'query'.");

		this.name = name;
		this.query = query;
	}

	void setParent(Catalog parent) {
		if (parent == null && this.parent != null) {
			checkState(!this.parent.containsView(name),
					"The view: " + name + " is still present in the catalog: " + this.parent.getName() + ".");
		}
		else if (parent != null && this.parent == null) {
			checkState(!parent.containsTable(name),
					"The catalog: " + parent.getName() + " already contains a different table with the name: " + name);
			checkState(!parent.containsView(name),
					"The catalog: " + parent.getName() + " already contains a different view with the name: " + name);
		}

		this.parent = parent;
	}

	public View rename(String newName) {
		checkArgument(!Strings.isNullOrEmpty(newName), "You must specify a 'name'.");
		if (parent != null) {
			checkState(!parent.containsTable(newName),
					"Catalog: " + parent.getName() + " already contains view with name: " + newName);
		}

		this.name = newName;
		return this;
	}

	@Override
	public View copy() {
		View copy = new View(name, query);
		copy.recursive = recursive;
		copy.temporary = temporary;
		return copy;
	}

	@Override
	public int compareTo(View o) {
		return name.compareTo(o.name);
	}

	@Override
	public String toString() {
		return PrettyPrinter.prettyPrint(this);
	}

}
