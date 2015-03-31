package io.quantumdb.core.backends.postgresql.migrator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.Identity;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.utils.QueryBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NullRecordPurger {

	public void purgeNullObjects(Connection connection, Map<Table, Identity> nullObjects) throws SQLException {
		ensureDeferredConstraints(connection);

		log.debug("Purging NULL objects for tables: " + nullObjects.keySet());
		for (Entry<Table, Identity> nullObject : nullObjects.entrySet()) {
			purgeNullObject(connection, nullObject);
		}

		commit(connection);
	}

	private void purgeNullObject(Connection connection, Entry<Table, Identity> nullObject) throws SQLException {
		Table table = nullObject.getKey();
		Identity identity = nullObject.getValue();

		QueryBuilder queryBuilder = new QueryBuilder("DELETE FROM " + table.getName() + " WHERE");
		queryBuilder.append(Joiner.on(" = ?, ").join(identity.keys()) + " = ?");

		try (PreparedStatement statement = connection.prepareStatement(queryBuilder.toString())) {
			int i = 0;
			for (String columnName : identity.keys()) {
				i++;
				Object value = identity.getValue(columnName);
				Column column = table.getColumn(columnName);
				ColumnType type = column.getType();
				type.getValueSetter().setValue(statement, i, value);
			}
			statement.execute();
		}
	}

	private void ensureDeferredConstraints(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		try (Statement statement = connection.createStatement()) {
			statement.execute("SET CONSTRAINTS ALL DEFERRED;");
		}
	}

	private void commit(Connection connection) throws SQLException {
		connection.commit();
		connection.setAutoCommit(true);
	}

}
