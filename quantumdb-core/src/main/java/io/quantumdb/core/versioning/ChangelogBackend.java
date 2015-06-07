package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.ChangeLogDataBackend.ChangeLogEntry;
import io.quantumdb.core.versioning.ChangeSetDataBackend.ChangeSetEntry;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter(AccessLevel.NONE)
public class ChangelogBackend {

	public Changelog load(Backend backend) throws SQLException {
		try (Connection connection = backend.connect()) {
			BackendUtils.ensureQuantumDbTablesExist(backend, connection);
			return BackendUtils.inTransaction(connection, () -> {
				Changelog log = null;

				ChangeLogDataBackend changeLogBackend = new ChangeLogDataBackend();
				ChangeSetDataBackend changeSetBackend = new ChangeSetDataBackend();
				Map<String, ChangeLogEntry> changeLogEntries = changeLogBackend.load(backend, connection);
				Map<String, ChangeSetEntry> changeSetEntries = changeSetBackend.load(backend, connection);

				for (ChangeLogEntry entry : changeLogEntries.values()) {
					ChangeSetEntry changeSetEntry = changeSetEntries.get(entry.getVersionId());

					ChangeSet changeSet;
					if (changeSetEntry != null) {
						changeSet = new ChangeSet(changeSetEntry.getAuthor(), changeSetEntry.getCreated(),
								changeSetEntry.getDescription());
					}
					else {
						Version parent = log.getVersion(entry.getParentVersionId());
						changeSet = parent.getChangeSet();
					}

					if (log == null) {
						log = new Changelog(entry.getVersionId(), changeSet);
						changeSet.setVersion(log.getRoot());
					}
					else {
						String parentVersionId = entry.getParentVersionId();

						Version parent = log.getVersion(parentVersionId);
						SchemaOperation schemaOperation = entry.getSchemaOperation();
						log.addChangeSet(parent, entry.getVersionId(), changeSet, schemaOperation);

						if (changeSet.getVersion() == null) {
							changeSet.setVersion(log.getVersion(entry.getVersionId()));
						}
					}
				}

				if (log == null) {
					String author = System.getProperty("user.name");
					ChangeSet changeSet = new ChangeSet(author, "Initial import of existing database.");
					log = new Changelog(RandomHasher.generateHash(), changeSet);
				}
				return log;
			});
		}
	}

	public void persist(Backend backend, Changelog changelog) throws SQLException {
		try (Connection connection = backend.connect()) {
			BackendUtils.ensureQuantumDbTablesExist(backend, connection);
			BackendUtils.inTransaction(connection, () -> {
				ChangeLogDataBackend changeLogBackend = new ChangeLogDataBackend();
				ChangeSetDataBackend changeSetBackend = new ChangeSetDataBackend();
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
