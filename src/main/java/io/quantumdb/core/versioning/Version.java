package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.utils.RandomHasher;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import lombok.ToString;


/**
 * This class can be used to describe the evolution of the database at a given point in time. It resembles much of the
 * idea of a commit in the distributed version control system Git, in such that it has a parent and that it holds
 * information on the difference between the parent Version and this Version.
 */
@Data
@ToString(of = { "id" })
public class Version {

	static Version rootVersion() {
		RandomHasher hasher = new RandomHasher();
		return new Version(hasher.generate(), null);
	}

	private final String id;

	@Setter(AccessLevel.PRIVATE)
	private Version parent;

	@Setter(AccessLevel.PRIVATE)
	private Version child;

	@Setter(AccessLevel.PRIVATE)
	private ChangeSet changeSet;

	@Setter(AccessLevel.PRIVATE)
	private SchemaOperation schemaOperation;

	/**
	 * Creates a new Version object based on the specified parameters.
	 *
	 * @param id     The unique identifier of this Version object.
	 * @param parent The parent of this Version object (may be NULL in case of a root Version).
	 */
	Version(String id, Version parent) {
		checkArgument(id != null, "You must specify a 'id'.");

		this.id = id;
		if (parent != null) {
			linkToParent(parent);
		}
	}

	/**
	 * Sets the parent of this Version object. Note that you may only set the parent once.
	 *
	 * @param parent The parent Version of this Version.
	 */
	void linkToParent(Version parent) {
		checkArgument(parent != null, "You must specify a 'parent'.");
		checkState(this.parent == null, "You have already specified a 'parent' previously.");

		this.parent = parent;
		parent.addChild(this);
	}

	private void addChild(Version child) {
		checkArgument(child != null, "You must specify a 'child'.");

		this.child = child;
	}

	/**
	 * Applies a SchemaOperation belonging to a specified ChangeSet to this Version object. The result is a new Version
	 * object which has this object as a parent Version. It also keeps a reference to the ChangeSet to which it belongs
	 * and stores a reference to the specified SchemaOperation.
	 *
	 * @param operation The SchemaOperation to apply to this Version.
	 * @param changeSet The ChangeSet to which this SchemaOperation belongs.
	 *
	 * @return The next Version which describes this change based on this Version.
	 */
	public Version apply(SchemaOperation operation, ChangeSet changeSet) {
		checkArgument(operation != null, "You must specify a 'operation'.");
		checkArgument(changeSet != null, "You must specify a 'changeSet'.");

		RandomHasher hasher = new RandomHasher();
		Version version = new Version(hasher.generate(), this);
		version.setSchemaOperation(operation);
		version.setChangeSet(changeSet);
		return version;
	}


}
