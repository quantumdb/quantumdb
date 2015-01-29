package io.quantumdb.core.migration.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.quantumdb.core.versioning.Version;

public class VersionTraverser {

	public static Set<Version> enumerateChildren(Version version) {
		List<Version> toVisit = Lists.newArrayList(version);
		Set<Version> children = Sets.newHashSet();

		while (!toVisit.isEmpty()) {
			Version current = toVisit.remove(0);

			Version child = current.getChild();
			if (child != null) {
				children.add(child);
				toVisit.add(child);
			}
		}

		return children;
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

}
