package io.quantumdb.core.migration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.quantumdb.core.migration.Migrator.Stage;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.schema.operations.Operation.Type;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VersionTraverser {

	public enum Direction {
		BACKWARDS, FORWARDS;
	}

	public static Direction getDirection(Version from, Version to) {
		checkArgument(from != null, "You must specify a 'from' version!");
		checkArgument(to != null, "You must specify a 'to' version!");

		int fromGeneration = getGeneration(from);
		int toGeneration = getGeneration(to);

		if (fromGeneration > toGeneration) {
			return Direction.BACKWARDS;
		}
		else if (toGeneration > fromGeneration) {
			return Direction.FORWARDS;
		}
		throw new IllegalArgumentException("These are different versions but in the same generation: " + from + ", " + to);
	}

	public static Version getFirst(Set<Version> versions) {
		checkArgument(versions != null, "You must specify a set of 'versions'!");

		Version first = null;
		int lowestGeneration = Integer.MAX_VALUE;
		for (Version version : versions) {
			int generation = getGeneration(version);
			if (generation < lowestGeneration) {
				lowestGeneration = generation;
				first = version;
			}
		}
		return first;
	}

	private static int getGeneration(Version version) {
		int generation = 0;
		Version pointer = version;
		while (pointer.getParent() != null) {
			pointer = pointer.getParent();
			generation++;
		}
		return generation;
	}

	public static Optional<List<Version>> findPath(Version from, Version to) {
		return findPath(from, to, true, true);
	}

	public static Optional<List<Version>> findChildPath(Version from, Version to) {
		return findPath(from, to, true, false);
	}

	public static Optional<List<Version>> findParentPath(Version from, Version to) {
		return findPath(from, to, false, true);
	}

	private static Optional<List<Version>> findPath(Version from, Version to,
			boolean traverseChildren, boolean traverseParent) {

		checkArgument(from != null, "You must specify a 'from'.");
		checkArgument(to != null, "You must specify a 'to'.");

		if (from.equals(to)) {
			return Optional.of(Lists.newArrayList(from));
		}

		Map<Version, Version> closest = Maps.newHashMap();
		LinkedList<Version> toProcess = Lists.newLinkedList();
		toProcess.add(from);

		boolean pathExists = false;
		while (!toProcess.isEmpty()) {
			Version current = toProcess.removeFirst();
			List<Version> adjacentVersions = Lists.newArrayList();

			if (traverseChildren && current.getChild() != null) {
				adjacentVersions.add(current.getChild());
			}
			if (traverseParent && current.getParent() != null) {
				adjacentVersions.add(current.getParent());
			}

			adjacentVersions.removeAll(closest.keySet());
			for (Version adjacentVersion : adjacentVersions) {
				closest.putIfAbsent(adjacentVersion, current);

				if (adjacentVersion.equals(to)) {
					pathExists = true;
				}
			}

			if (pathExists) {
				toProcess.clear();
				break;
			}

			toProcess.addAll(adjacentVersions);
		}

		if (!pathExists) {
			return Optional.empty();
		}

		LinkedList<Version> path = Lists.newLinkedList();
		path.add(to);

		while (!path.getLast().equals(from)) {
			path.add(closest.get(path.getLast()));
		}

		return Optional.of(Lists.reverse(path));
	}

	public static List<Stage> verifyPathAndState(State state, Version from, Version to) {
		RefLog refLog = state.getRefLog();
		Set<Version> versions = refLog.getVersions();

		if (versions.isEmpty()) {
			checkState(from.isRoot(), "Database is not initialized yet, you must start migrating from the root node.");
		}
		else {
			boolean atCorrectVersion = versions.contains(from);
			checkState(atCorrectVersion, "The database is not at version: '" + from + "' but at: '" + versions + "'.");
		}

		List<Version> path = VersionTraverser.findChildPath(from, to)
				.orElseThrow(() -> new IllegalStateException("No path from " + from.getId() + " to " + to.getId()));

		Version pointer = null;
		List<Stage> stages = Lists.newArrayList();
		for (Version currentStep : path) {
			if (pointer == null) {
				// First step in the changelog...
				pointer = currentStep;
				continue;
			}

			Operation operation = currentStep.getOperation();
			Type previous = stages.isEmpty() ? null : stages.get(stages.size() - 1).getType();

			if (operation.getType() != previous || stages.isEmpty()) {
				stages.add(new Stage(operation.getType(), Lists.newArrayList(currentStep), pointer));
			}
			else {
				stages.get(stages.size() - 1).addVersion(currentStep);
			}

			pointer = currentStep;
		}

		return stages;
	}

}
