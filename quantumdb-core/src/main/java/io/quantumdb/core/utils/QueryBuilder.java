package io.quantumdb.core.utils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class to build up queries without having to concern yourself with white-spacing
 * between append() method calls.
 */
public class QueryBuilder {

	private final StringBuilder builder;

	/**
	 * Constructs an empty QueryBuilder object.
	 */
	public QueryBuilder() {
		this.builder = new StringBuilder();
	}

	/**
	 * Constructs a new QueryBuilder object initialized with the specified part.
	 *
	 * @param part The first part of the query.
	 */
	public QueryBuilder(String part) {
		checkArgument(part != null, "You must specify a 'part'.");
		this.builder = new StringBuilder(part);
	}

	/**
	 * Appends a specified part to the end of the query.
	 *
	 * @param part The part to add to the query.
	 *
	 * @return The QueryBuilder instance.
	 */
	public QueryBuilder append(String part) {
		checkArgument(part != null, "You must specify a 'part'.");

		if (builder.length() > 0) {
			builder.append(" ");
		}
		builder.append(part.trim());
		return this;
	}

	/**
	 * @return The full query.
	 */
	@Override
	public String toString() {
		return builder.toString();
	}

}
