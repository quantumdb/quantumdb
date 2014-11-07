package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.quantumdb.core.schema.operations.SchemaOperation;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;


/**
 * This class describes a list of SchemaOperations which batched together form a logical set of changes to the database
 * schema. This batch can optionally be given a description, much like a commit message in version control systems.
 */
@Data
public class ChangeSet {

	private final String description;

	@Getter(AccessLevel.NONE)
	private final List<SchemaOperation> schemaOperations;

	/**
	 * Creates a new ChangeSet object based on the specified array of SchemaOperations. This ChangeSet will have no
	 * description associated with it.
	 *
	 * @param operations A non-empty array of SchemaOperations that perform a logical set of operations.
	 */
	ChangeSet(SchemaOperation... operations) {
		this(null, operations);
	}

	/**
	 * Creates a new ChangeSet object based on the specified description and array of SchemaOperations.
	 *
	 * @param description The description of the ChangeSet (may be NULL).
	 * @param operations  A non-empty array of SchemaOperations that perform a logical set of operations.
	 */
	ChangeSet(String description, SchemaOperation... operations) {
		checkArgument(operations != null && operations.length > 0, "You must specify at least one schema operation.");

		this.description = Strings.emptyToNull(description);
		this.schemaOperations = Arrays.asList(operations);
	}

	public ImmutableList<SchemaOperation> getSchemaOperations() {
		return ImmutableList.copyOf(schemaOperations);
	}

}
