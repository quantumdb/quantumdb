package io.quantumdb.core.versioning;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import io.quantumdb.core.backends.Backend;

interface PrimaryKeyBackend<K, V> {
	Map<K, V> load(Backend backend, Connection connection) throws SQLException;
	V create(K key) throws SQLException;
	void delete(K key) throws SQLException;
	void persist() throws SQLException;
}
