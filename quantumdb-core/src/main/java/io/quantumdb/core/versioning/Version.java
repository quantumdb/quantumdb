package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import io.quantumdb.core.schema.operations.SchemaOperation;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.ToString;


/**
 * This class can be used to describe the evolution of the database at a given point in time. It resembles much of the
 * idea of a commit in the distributed version control system Git, in such that it has a parent and that it holds
 * information on the difference between the parent Version and this Version.
 */
@Data
@EqualsAndHashCode(of = { "id" })
@ToString(of = { "id" })
public class Version implements Comparable<Version> {

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
	public Version(String id, Version parent) {
		this (id, parent, null, null);
	}

	/**
	 * Creates a new Version object based on the specified parameters.
	 *
	 * @param id     The unique identifier of this Version object.
	 * @param parent The parent of this Version object (may be NULL in case of a root Version).
	 */
	public Version(String id, Version parent, ChangeSet changeSet, SchemaOperation schemaOperation) {
		checkArgument(!isNullOrEmpty(id), "You must specify a 'id'.");

		this.id = id;
		this.schemaOperation = schemaOperation;
		this.changeSet = changeSet;
		if (parent != null) {
			this.parent = parent;
			this.parent.child = this;
		}
	}

	@Override
	public int compareTo(Version other) {
		if (this.equals(other)) {
			return 0;
		}

		Version pointer = this;
		while (pointer != null) {
			if (pointer.equals(other)) {
				return 1;
			}
			pointer = pointer.getParent();
		}
		pointer = this;
		while (pointer != null) {
			if (pointer.equals(other)) {
				return -1;
			}
			pointer = pointer.getChild();
		}

		return 0;
	}

	public boolean isRoot() {
		return parent == null;
	}
}
