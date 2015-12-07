package io.quantumdb.core.migration;

import static com.google.common.base.Preconditions.checkState;

import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.migration.utils.VersionTraverser;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Migrator {

	private final Backend backend;

	public Migrator(Backend backend) {
		this.backend = backend;
	}

	public void migrate(String sourceVersionId, String targetVersionId) throws MigrationException {
		log.info("Migrating database structure from version: {} to version: {}", sourceVersionId, targetVersionId);

		State state = loadState();
		Changelog changelog = state.getChangelog();
		Version from = changelog.getVersion(sourceVersionId);
		Version to = changelog.getVersion(targetVersionId);
		Set<Version> versions = Sets.newHashSet(VersionTraverser.findPath(from, to).get());

		Set<String> origins = Sets.newHashSet(changelog.getRoot().getId());
		if (!versions.isEmpty()) {
			origins = versions.stream()
					.map(Version::getId)
					.collect(Collectors.toSet());
		}

		if (!origins.contains(sourceVersionId)) {
			log.warn("Not migrating database structure -> Not currently at version: {}", sourceVersionId);
			return;
		}

		if (origins.contains(targetVersionId)) {
			log.warn("Not migrating database structure -> Already at version: {}", targetVersionId);
			return;
		}

		verifyPathAndState(state, from, to);

		backend.getMigrator().migrate(state, from, to);
	}

	private State loadState() throws MigrationException {
		try {
			return backend.loadState();
		}
		catch (SQLException e) {
			throw new MigrationException("Could not load current state: " + e.getMessage(), e);
		}
	}

	private void verifyPathAndState(State state, Version from, Version to) {
		Set<Version> versions = Sets.newHashSet(VersionTraverser.findPath(from, to).get());

		if (!versions.isEmpty()) {
			boolean atCorrectVersion = versions.contains(from);
			checkState(atCorrectVersion, "The database is not at version: '" + from + "' but at: '" + versions + "'.");
		}
		else {
			checkState(from.isRoot(), "Database is not initialized yet, you must start migrating from the root node.");
		}

		VersionTraverser.findChildPath(from, to)
				.orElseThrow(() -> new IllegalStateException("No path from " + from.getId() + " to " + to.getId()));
	}

}
