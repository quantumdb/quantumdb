package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Queue;
import java.util.Set;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import io.quantumdb.core.utils.RandomHasher;

class VersionIdGenerator {

	private final Version rootVersion;
	private final Set<String> ids;

	VersionIdGenerator(Version rootVersion) {
		checkArgument(rootVersion.getParent() == null, "You must specify the 'rootVersion'.");

		this.rootVersion = rootVersion;
		this.ids = Sets.newHashSet(index());
	}

	private Set<String> index() {
		Set<String> visited = Sets.newHashSet();
		Queue<Version> toVisit = Queues.newLinkedBlockingDeque();
		toVisit.add(rootVersion);

		while (!toVisit.isEmpty()) {
			Version current = toVisit.poll();
			if (visited.add(current.getId()) && current.getChild() != null) {
				toVisit.add(current.getChild());
			}
		}

		return visited;
	}

	String generateId() {
		while (true) {
			String newId = RandomHasher.generateHash();
			if (!ids.contains(newId)) {
				ids.add(newId);
				return newId;
			}
		}
	}

}
