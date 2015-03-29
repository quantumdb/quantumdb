package io.quantumdb.core.backends.postgresql.integration.videostores;

import java.sql.SQLException;

import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.backends.postgresql.PostgresTypes;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Version;
import org.junit.Test;

public class ModifyingCustomersTable extends PostgresqlBaseScenarioTest {

	@Test
	public void testAddingNewColumnToTable() throws MigrationException, SQLException {
		Version currentVersion = getChangelog().getLastAdded();

		getChangelog().addChangeSet("Michael de Jong",
				SchemaOperations.addColumn("customers", "date_of_birth", PostgresTypes.date()));

		Version targetVersion = getChangelog().getLastAdded();
		getBackend().persistState(getState());

		getMigrator().migrate(currentVersion.getId(), targetVersion.getId());
	}

}
