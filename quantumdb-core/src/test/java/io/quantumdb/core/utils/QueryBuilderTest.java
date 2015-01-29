package io.quantumdb.core.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class QueryBuilderTest {

	@Test
	public void testConstructorWithNoInputs() {
		QueryBuilder builder = new QueryBuilder();
		assertEquals("", builder.toString());
	}

	@Test
	public void testConstructorWithBeginningOfQuery() {
		QueryBuilder builder = new QueryBuilder("SELECT");
		assertEquals("SELECT", builder.toString());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructorWithNullInputThrowsException() {
		new QueryBuilder(null);
	}

	@Test
	public void testConstructorWithEmptyInputIsAccepted() {
		QueryBuilder builder = new QueryBuilder("");
		assertEquals("", builder.toString());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullCannotBeAppended() {
		QueryBuilder builder = new QueryBuilder();
		builder.append(null);
	}

	@Test
	public void testAppendingMultipleEmptyStringsStillResultsInEmptyString() {
		QueryBuilder builder = new QueryBuilder();
		builder.append("");
		builder.append("");
		builder.append("");
		assertEquals("", builder.toString());
	}

	@Test
	public void testAppendingTrimmedStringsAddsWhitespace() {
		QueryBuilder builder = new QueryBuilder();
		builder.append("SELECT");
		builder.append("*");
		builder.append("FROM users");
		assertEquals("SELECT * FROM users", builder.toString());
	}

	@Test
	public void testAppendingNonTrimmedStringsDoNotAddWhitespace() {
		QueryBuilder builder = new QueryBuilder();
		builder.append("SELECT ");
		builder.append(" * ");
		builder.append(" FROM users");
		assertEquals("SELECT * FROM users", builder.toString());
	}

}
