package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which drops an existing view from a catalog.
 */
@Data
@Accessors(chain = true)
public class DropView implements SchemaOperation {

	private final String viewName;

	DropView(String viewName) {
		checkArgument(!Strings.isNullOrEmpty(viewName), "You must specify a 'viewName'.");

		this.viewName = viewName;
	}

}
