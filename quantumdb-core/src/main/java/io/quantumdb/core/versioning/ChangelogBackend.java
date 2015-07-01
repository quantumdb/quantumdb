package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.ChangeLogDataBackend.ChangeLogEntry;
import io.quantumdb.core.versioning.ChangeSetDataBackend.ChangeSetEntry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChangelogBackend {

	private final ChangeLogDataBackend changeLogBackend = new ChangeLogDataBackend();
	private final ChangeSetDataBackend changeSetBackend = new ChangeSetDataBackend();

	public Changelog load(Backend backend) throws SQLException {
		try (Connection connection = backend.connect()) {
			BackendUtils.ensureQuantumDbTablesExist(backend, connection);
			return BackendUtils.inTransaction(connection, () -> {
				Map<String, ChangeLogEntry> changeLogEntries = changeLogBackend.load(backend, connection);
				Map<String, ChangeSetEntry> changeSetEntries = changeSetBackend.load(backend, connection);

				Optional<ChangeLogEntry> root = changeLogEntries.values().stream()
						.filter(entry -> entry.getParentVersionId() == null)
						.findAny();

				if (root.isPresent()) {
					ChangeLogEntry logEntry = root.get();
					String rootVersionId = logEntry.getVersionId();

					Map<String, ChangeSet> sets = changeSetEntries.values().stream()
							.collect(Collectors.toMap(ChangeSetEntry::getVersionId,
									entry -> new ChangeSet(entry.getAuthor(), entry.getCreated(),
											entry.getDescription())));

					Map<String, Version> versions = Maps.newLinkedHashMap();
					Set<ChangeLogEntry> entries = Sets.newHashSet(changeLogEntries.values());
					while (!entries.isEmpty()) {
						Set<ChangeLogEntry> toRemove = Sets.newHashSet();
						for (ChangeLogEntry entry : entries) {
							String currentVersionId = entry.getVersionId();
							String parentVersionId = entry.getParentVersionId();
							if (parentVersionId == null) {
								toRemove.add(entry);
								ChangeSet changeSet = sets.get(currentVersionId);

								Version newVersion = new Version(currentVersionId, null, changeSet, entry.getSchemaOperation());
								versions.put(currentVersionId, newVersion);
							}
							else {
								Version parentVersion = versions.get(parentVersionId);
								if (parentVersion != null) {
									toRemove.add(entry);
									ChangeSet changeSet = Optional.ofNullable(sets.get(currentVersionId))
											.orElse(parentVersion.getChangeSet());

									Version newVersion = new Version(currentVersionId, parentVersion, changeSet, entry.getSchemaOperation());
									versions.put(currentVersionId, newVersion);
								}
							}
						}
						entries.removeAll(toRemove);
					}

					Changelog log = new Changelog(rootVersionId, sets.get(rootVersionId));
					for (Version version : versions.values()) {
						Optional.ofNullable(version.getParent())
								.map(Version::getId)
								.map(log::getVersion)
								.ifPresent(parent -> log.addChangeSet(parent, version.getId(), version.getChangeSet(), version.getSchemaOperation()));
					}
					return log;
				}

				String author = System.getProperty("user.name");
				ChangeSet changeSet = new ChangeSet(author, "Initial import of existing database.");
				return new Changelog(RandomHasher.generateHash(), changeSet);
			});
		}
	}

	public void persist(Backend backend, Changelog changelog) throws SQLException {
		try (Connection connection = backend.connect()) {
			BackendUtils.ensureQuantumDbTablesExist(backend, connection);
			BackendUtils.inTransaction(connection, () -> {
				Map<String, ChangeLogEntry> changeLogEntries = changeLogBackend.load(backend, connection);
				Map<String, ChangeSetEntry> changeSetEntries = changeSetBackend.load(backend, connection);

				Set<String> processed = Sets.newHashSet();
				Set<String> updateChangeSets = Sets.newHashSet();

				Queue<Version> queue = Queues.newLinkedBlockingQueue();
				queue.add(changelog.getRoot());
				while (!queue.isEmpty()) {
					Version version = queue.poll();
					SchemaOperation schemaOperation = version.getSchemaOperation();

					ChangeLogEntry changeLogEntry;
					if (changeLogEntries.containsKey(version.getId())) {
						changeLogEntry = changeLogEntries.get(version.getId());
					}
					else {
						changeLogEntry = changeLogBackend.create(version.getId());
					}

					changeLogEntry.setSchemaOperation(schemaOperation);
					Version parent = version.getParent();
					changeLogEntry.setParentVersionId(parent != null ? parent.getId() : null);

					ChangeSet changeSet = version.getChangeSet();
					if (changeSet != null && !updateChangeSets.contains(changeSet.getVersion().getId())) {
						ChangeSetEntry changeSetEntry;
						if (changeSetEntries.containsKey(version.getId())) {
							changeSetEntry = changeSetEntries.get(version.getId());
						}
						else {
							changeSetEntry = changeSetBackend.create(version.getId());
						}

						changeSetEntry.setAuthor(changeSet.getAuthor());
						changeSetEntry.setCreated(changeSet.getCreated());
						changeSetEntry.setDescription(changeSet.getDescription());

						updateChangeSets.add(changeSet.getVersion().getId());
					}

					processed.add(version.getId());

					Version child = version.getChild();
					if (child != null && !processed.contains(child.getId()) && !queue.contains(child.getId())) {
						queue.add(child);
					}
				}

				changeLogBackend.persist();
				changeSetBackend.persist();

				return null;
			});
		}
	}

}
