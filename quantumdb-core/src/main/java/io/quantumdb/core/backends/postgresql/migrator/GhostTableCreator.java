package io.quantumdb.core.backends.postgresql.migrator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;

public class GhostTableCreator {

	private final TableCreator tableCreator;
	private final NullRecordCreator nullRecordCreator;

	public GhostTableCreator() {
		this.tableCreator = new TableCreator();
		this.nullRecordCreator = new NullRecordCreator();
	}

	public Map<Table, Identity> create(Connection connection, Collection<Table> tables) throws SQLException {
		tableCreator.createTables(connection, tables);
		Map<Table, Identity> identities = nullRecordCreator.insertNullObjects(connection, Lists.newArrayList(tables));
		tableCreator.createForeignKeys(connection, tables);
		return identities;
	}

}
