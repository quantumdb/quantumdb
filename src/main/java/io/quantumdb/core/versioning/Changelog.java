package io.quantumdb.core.versioning;

import io.quantumdb.core.schema.operations.SchemaOperation;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;


/**
 * This class allows you to define a series of successive ChangeSets, building up a changelog in the process.
 */
@Data
public class Changelog {

	private final Version root;

	@Setter(AccessLevel.NONE)
	@Getter(AccessLevel.NONE)
	private Version pointer;

	/**
	 * Creates a new Changelog object.
	 */
	public Changelog() {
		this.root = Version.rootVersion();
		this.pointer = this.root;
	}

	/**
	 * @return The Version object last appended to this Changelog object.
	 */
	public Version getCurrent() {
		return pointer;
	}

	/**
	 * Adds a new ChangeSet to the Changelog object, based on the specified parameters.
	 *
	 * @param operations A non-empty array of SchemaOperations that perform a logical set of operations.
	 *
	 * @return The constructed ChangeSet object.
	 */
	public ChangeSet addChangeSet(SchemaOperation... operations) {
		return addChangeSet(null, operations);
	}

	/**
	 * Adds a new ChangeSet to the Changelog object, based on the specified parameters.
	 *
	 * @param description The description of the ChangeSet (may be NULL).
	 * @param operations  A non-empty array of SchemaOperations that perform a logical set of operations.
	 *
	 * @return The constructed ChangeSet object.
	 */
	public ChangeSet addChangeSet(String description, SchemaOperation... operations) {
		ChangeSet changeSet = new ChangeSet(description, operations);
		for (SchemaOperation operation : operations) {
			this.pointer = pointer.apply(operation, changeSet);
		}
		return changeSet;
	}

}
