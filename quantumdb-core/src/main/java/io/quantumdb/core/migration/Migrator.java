package io.quantumdb.core.migration;

import static com.google.common.base.Preconditions.checkState;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.DatabaseMigrator;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.schema.operations.Operation.Type;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Migrator {

	@Data
	public static class Stage {

		private final Type type;
		private final List<Version> versions;
		private final Version parent;

		public Version getParent() {
			return parent;
		}

		public ImmutableList<Version> getVersions() {
			return ImmutableList.copyOf(versions);
		}

		void addVersion(Version version) {
			checkState(versions.isEmpty() || versions.contains(version.getParent()),
					"The specified Version's parent must be present in the Stage!");

			versions.add(version);
		}

		public Version getFirst() {
			checkState(!versions.isEmpty(), "A Stage must contain at least one version!");
			return versions.get(0);
		}

		public Version getLast() {
			checkState(!versions.isEmpty(), "A Stage must contain at least one version!");
			return versions.get(versions.size() - 1);
		}

	}

	private final Backend backend;

	public Migrator(Backend backend) {
		this.backend = backend;
	}

	public void migrate(String sourceVersionId, String targetVersionId) throws MigrationException {
		log.info("Forking from version: {} to version: {}", sourceVersionId, targetVersionId);

		State state = loadState();
		Changelog changelog = state.getChangelog();
		Version from = changelog.getVersion(sourceVersionId);
		Version to = changelog.getVersion(targetVersionId);

		Set<String> origins = Sets.newHashSet(changelog.getRoot().getId());
		if (!origins.contains(sourceVersionId)) {
			log.warn("Not forking database, since we're not currently at version: {}", sourceVersionId);
			return;
		}

		if (origins.contains(targetVersionId)) {
			log.warn("Not forking database, since we're already at version: {}", targetVersionId);
			return;
		}

		DatabaseMigrator migrator = backend.getMigrator();
		List<Stage> stages = VersionTraverser.verifyPathAndState(state, from, to);

		Version intermediate = null;
		for (Stage stage : stages) {
			if (stage.getType() == Type.DDL) {
				log.info("Creating new state: {}", stage.getLast());
				migrator.applySchemaChanges(state, stage.getParent(), stage.getLast());
				if (intermediate != null) {
					log.info("Dropping intermediate state: {}", intermediate.getId());
					migrator.drop(state, intermediate);
				}
				intermediate = stage.getLast();
			}
			else  if (stage.getType() == Type.DML) {
				log.info("Executing data changes: {}", stage.getVersions());
				migrator.applyDataChanges(state, stage);
			}
		}
	}

	private State loadState() throws MigrationException {
		try {
			return backend.loadState();
		}
		catch (SQLException e) {
			throw new MigrationException("Could not load current state: " + e.getMessage(), e);
		}
	}

}
