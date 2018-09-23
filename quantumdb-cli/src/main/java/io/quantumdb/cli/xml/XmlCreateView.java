package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import io.quantumdb.core.schema.operations.CreateView;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlCreateView implements XmlOperation<CreateView> {

	static final String TAG = "createView";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlCreateView operation = new XmlCreateView();
		operation.setViewName(element.getAttributes().get("viewName"));
		operation.setRecursive(Boolean.TRUE.toString().equals(element.getAttributes().get("recursive")));
		operation.setTemporary(Boolean.TRUE.toString().equals(element.getAttributes().get("temporary")));
		operation.setQuery(Strings.emptyToNull(element.getChildren().stream()
				.filter(child -> child.getTag().equals(XmlQuery.TAG))
				.map(child -> (XmlQuery) XmlQuery.convert(child))
				.map(XmlQuery::getQuery)
				.findFirst()
				.orElseThrow(() -> new RuntimeException("No query specified!"))
				.trim()));

		return operation;
	}

	private String viewName;
	private boolean recursive;
	private boolean temporary;
	private String query;

	@Override
	public CreateView toOperation() {
		CreateView operation = SchemaOperations.createView(viewName);
		if (recursive) {
			operation.recursive();
		}
		if (temporary) {
			operation.temporary();
		}
		operation.as(query);
		return operation;
	}

}
