package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.DataOperation;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlQuery implements XmlOperation<DataOperation> {

	static final String TAG = "sql";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlQuery operation = new XmlQuery();
		operation.setQuery(element.getText());
		return operation;
	}

	private String query;

	@Override
	public DataOperation toOperation() {
		return SchemaOperations.execute(query);
	}

}
