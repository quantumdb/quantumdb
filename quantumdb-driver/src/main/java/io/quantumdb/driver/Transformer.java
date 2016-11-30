package io.quantumdb.driver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.quantumdb.query.rewriter.QueryRewriter;

class Transformer {

	private final QueryRewriter queryRewriter;
	private final Map<String, String> tableMapping;

	Transformer(Connection connection, QueryRewriter queryRewriter, String version) throws SQLException {
		this.tableMapping = new HashMap<>();
		this.queryRewriter = queryRewriter;

		if (version != null && !version.isEmpty()) {
			String query = new StringBuilder()
					.append("SELECT table_id, table_name ")
					.append("FROM quantumdb_table_versions ")
					.append("WHERE version_id = ?;")
					.toString();

			try (PreparedStatement statement = connection.prepareStatement(query)) {
				statement.setString(1, version);

				try (ResultSet resultSet = statement.executeQuery()) {
					while (resultSet.next()) {
						String tableId = resultSet.getString("table_id");
						String tableName = resultSet.getString("table_name");
						tableMapping.put(tableName, tableId);
					}
				}
			}
		}

		queryRewriter.setTableMapping(tableMapping);
	}

	Map<String, String> getTableMappings() {
		Map<String, String> result = new HashMap<>(tableMapping.size());
		result.putAll(tableMapping);
		return result;
	}

	String getTableId(String tableName) {
		return tableMapping.get(tableName);
	}

	String rewriteQuery(String query) throws SQLException {
		return queryRewriter.rewrite(query);
	}

}
