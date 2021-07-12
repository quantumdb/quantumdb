package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.DropView;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlDropView implements XmlOperation<DropView> {

	static final String TAG = "dropView";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlDropView operation = new XmlDropView();
		operation.setViewName(element.getAttributes().remove("viewName"));

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String viewName;

	@Override
	public DropView toOperation() {
		return SchemaOperations.dropView(viewName);
	}

}
