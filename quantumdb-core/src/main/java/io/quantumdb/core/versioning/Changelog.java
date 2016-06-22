package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.utils.RandomHasher;
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

	@Getter(AccessLevel.NONE)
	private final VersionIdGenerator idGenerator;

	@Setter(AccessLevel.NONE)
	private Version lastAdded;

	/**
	 * Creates a new Changelog object with a new root Version object.
	 */
	public Changelog() {
		this(RandomHasher.generateHash());
	}

	/**
	 * Creates a new Changelog object with a new root Version object with the specified id.
	 */
	public Changelog(String rootVersionId) {
		this(rootVersionId, null);
	}

	/**
	 * Creates a new Changelog object with a new root Version object with the specified id.
	 */
	public Changelog(String rootVersionId, ChangeSet changeSet) {
		this.root = new Version(rootVersionId, null, changeSet, null);
		this.idGenerator = new VersionIdGenerator(root);
		this.lastAdded = root;

		if (changeSet != null) {
			changeSet.setVersion(root);
		}
	}

	public Changelog addChangeSet(String author, Collection<Operation> operations) {
		return addChangeSet(lastAdded, new ChangeSet(author, new Date(), null), operations);
	}

	public Changelog addChangeSet(String author, Operation... operations) {
		return addChangeSet(lastAdded, new ChangeSet(author, new Date(), null), operations);
	}

	public Changelog addChangeSet(String author, String description, Collection<Operation> operations) {
		return addChangeSet(lastAdded, new ChangeSet(author, new Date(), description), operations);
	}

	public Changelog addChangeSet(String author, String description, Operation... operations) {
		return addChangeSet(lastAdded, new ChangeSet(author, new Date(), description), operations);
	}

	public Changelog addChangeSet(ChangeSet changeSet, Collection<Operation> operations) {
		return addChangeSet(lastAdded, changeSet, operations);
	}

	public Changelog addChangeSet(ChangeSet changeSet, Operation... operations) {
		return addChangeSet(lastAdded, changeSet, operations);
	}

	public Changelog addChangeSet(Version appendTo, ChangeSet changeSet, Operation... operations) {
		return addChangeSet(appendTo, changeSet, Lists.newArrayList(operations));
	}

	/**
	 * Adds a new ChangeSet to the Changelog object.
	 *
	 * @param appendTo The Version to append the ChangeSet to.
	 * @param changeSet The ChangeSet to add to this Changelog object.
	 * @param operations The Collection of Operations associated with this ChangeSet.
	 *
	 * @return The Changelog object.
	 */
	public Changelog addChangeSet(Version appendTo, ChangeSet changeSet, Collection<Operation> operations) {
		lastAdded = appendTo;

		for (Operation operation : operations) {
			lastAdded = new Version(idGenerator.generateId(), lastAdded, changeSet, operation);
		}

		changeSet.setVersion(lastAdded);
		return this;
	}

	/**
	 * Adds a new ChangeSet to the Changelog object.
	 *
	 * @param appendTo The Version to append the ChangeSet to.
	 * @param versionId The Version ID of this change.
	 * @param changeSet The ChangeSet to add to this Changelog object.
	 * @param operation The Operation associated with this version.
	 *
	 * @return The Changelog object.
	 */
	Changelog addChangeSet(Version appendTo, String versionId, ChangeSet changeSet, Operation operation) {
		lastAdded = new Version(versionId, appendTo, changeSet, operation);
		changeSet.setVersion(lastAdded);
		return this;
	}

	public Version getVersion(String versionId) {
		checkArgument(!isNullOrEmpty(versionId), "You must specify a 'versionId'.");

		List<Version> toVisit = Lists.newArrayList(root);
		Set<Version> visited = Sets.newHashSet();

		while (!toVisit.isEmpty()) {
			Version version = toVisit.remove(0);
			if (visited.contains(version)) {
				continue;
			}

			if (version.getId().equals(versionId)) {
				return version;
			}

			visited.add(version);

			if (version.getChild() != null) {
				toVisit.add(version.getChild());
			}
		}

		throw new IllegalArgumentException("No version found with id: '" + versionId + "'.");
	}
}
