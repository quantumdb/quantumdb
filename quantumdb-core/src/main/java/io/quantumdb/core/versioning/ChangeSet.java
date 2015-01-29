package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.google.common.collect.ImmutableList;
import io.quantumdb.core.schema.operations.SchemaOperation;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;


/**
 * This class describes a list of SchemaOperations which batched together form a logical set of changes to the database
 * schema. This batch can optionally be given a description, much like a commit message in version control systems.
 */
@Data
public class ChangeSet implements Comparable<ChangeSet> {

	private final String author;

	private final Date created;

	private final String description;

	@Setter(AccessLevel.PACKAGE)
	private Version version;

	/**
	 * Creates a new ChangeSet object based on the specified description and array of SchemaOperations.
	 *
	 * @param author The author of the ChangeSet.
	 */
	public ChangeSet(String author) {
		this (author, null);
	}

	/**
	 * Creates a new ChangeSet object based on the specified description and array of SchemaOperations.
	 *
	 * @param author The author of the ChangeSet.
	 * @param description The description of the ChangeSet (may be NULL).
	 */
	public ChangeSet(String author, String description) {
		this (author, new Date(), description);
	}

	/**
	 * Creates a new ChangeSet object based on the specified description and array of SchemaOperations.
	 *
	 * @param author The author of the ChangeSet.
	 * @param created The time of creation of this ChangeSet.
	 * @param description The description of the ChangeSet (may be NULL).
	 */
	ChangeSet(String author, Date created, String description) {
		checkArgument(!isNullOrEmpty(author), "You must specify an 'author'.");
		checkArgument(created != null, "You must specify a 'created' Date.");

		this.author = author;
		this.created = created;
		this.description = emptyToNull(description);
	}

	@Override
	public int compareTo(ChangeSet o) {
		return Comparator.comparing(ChangeSet::getCreated).compare(this, o);
	}
}
