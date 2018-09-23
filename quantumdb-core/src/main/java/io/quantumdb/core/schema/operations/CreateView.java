package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which creates a new view.
 */
@Data
@Accessors(chain = true)
public class CreateView implements SchemaOperation {

	private final String viewName;

	@Setter(AccessLevel.NONE)
	private boolean temporary = false;

	@Setter(AccessLevel.NONE)
	private boolean recursive = false;

	@Setter(AccessLevel.NONE)
	private String query;

	CreateView(String viewName) {
		checkArgument(!Strings.isNullOrEmpty(viewName), "You must specify a 'viewName'.");
		this.viewName = viewName;
	}

	public CreateView temporary() {
		this.temporary = true;
		return this;
	}

	public CreateView recursive() {
		this.recursive = true;
		return this;
	}

	public CreateView as(String query) {
		this.query = query;
		return this;
	}

}
