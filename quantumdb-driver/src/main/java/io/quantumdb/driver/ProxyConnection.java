package io.quantumdb.driver;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ProxyConnection implements Connection {

	private final Connection connection;
	private final Transformer transformer;

	ProxyConnection(Connection connection, Transformer transformer) {
		this.connection = connection;
		this.transformer = transformer;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new ProxyStatement(this, connection.createStatement(), transformer);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		String transformedSql = transformer.rewriteQuery(sql);
		PreparedStatement statement = connection.prepareStatement(transformedSql);
		return new ProxyPreparedStatement(this, statement, transformer);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		String transformedSql = transformer.rewriteQuery(sql);
		CallableStatement statement = connection.prepareCall(transformedSql);
		return new ProxyCallableStatement(this, statement, transformer);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		String transformedSql = transformer.rewriteQuery(sql);
		return connection.nativeSQL(transformedSql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		connection.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return connection.getAutoCommit();
	}

	@Override
	public void commit() throws SQLException {
		connection.commit();
	}

	@Override
	public void rollback() throws SQLException {
		connection.rollback();
	}

	@Override
	public void close() throws SQLException {
		connection.close();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return connection.isClosed();
	}

	@Override
	// TODO: See if we need to adopt this for core...
	public DatabaseMetaData getMetaData() throws SQLException {
		return new ProxyDatabaseMetaData(this, connection.getMetaData(), transformer);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		connection.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return connection.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		connection.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return connection.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		connection.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return connection.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return connection.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		connection.clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		Statement statement = connection.createStatement(resultSetType, resultSetConcurrency);
		return new ProxyStatement(this, statement, transformer);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {

		String transformedSql = transformer.rewriteQuery(sql);
		PreparedStatement statement = connection.prepareStatement(transformedSql, resultSetType, resultSetConcurrency);
		return new ProxyPreparedStatement(this, statement, transformer);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		String transformedSql = transformer.rewriteQuery(sql);
		CallableStatement statement = connection.prepareCall(transformedSql, resultSetType, resultSetConcurrency);
		return new ProxyCallableStatement(this, statement, transformer);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return connection.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		connection.setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		connection.setHoldability(holdability);
	}

	@Override
	public int getHoldability() throws SQLException {
		return connection.getHoldability();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return connection.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return connection.setSavepoint(name);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		connection.rollback(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		connection.releaseSavepoint(savepoint);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {

		Statement statement = connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
		return new ProxyStatement(this, statement, transformer);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {

		String transformedSql = transformer.rewriteQuery(sql);
		PreparedStatement statement = connection.prepareStatement(transformedSql, resultSetType, resultSetConcurrency,
				resultSetHoldability);

		return new ProxyPreparedStatement(this, statement, transformer);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {

		String transformedSql = transformer.rewriteQuery(sql);
		CallableStatement transformedStatement = connection.prepareCall(transformedSql, resultSetType,
				resultSetConcurrency, resultSetHoldability);

		return new ProxyCallableStatement(this, transformedStatement, transformer);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		String transformedSql = transformer.rewriteQuery(sql);
		PreparedStatement transformedStatement = connection.prepareStatement(transformedSql, autoGeneratedKeys);
		return new ProxyPreparedStatement(this, transformedStatement, transformer);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		String transformedSql = transformer.rewriteQuery(sql);
		PreparedStatement transformedStatement = connection.prepareStatement(transformedSql, columnIndexes);
		return new ProxyPreparedStatement(this, transformedStatement, transformer);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		String transformedSql = transformer.rewriteQuery(sql);
		PreparedStatement transformedStatement = connection.prepareStatement(transformedSql, columnNames);
		return new ProxyPreparedStatement(this, transformedStatement, transformer);
	}

	@Override
	public Clob createClob() throws SQLException {
		return connection.createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		return connection.createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return connection.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return connection.createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return connection.isValid(timeout);
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		connection.setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		connection.setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return connection.getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return connection.getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return connection.createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return connection.createStruct(typeName, attributes);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		connection.setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		return connection.getSchema();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		connection.abort(executor);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		connection.setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return connection.getNetworkTimeout();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return connection.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return connection.isWrapperFor(iface);
	}

}
