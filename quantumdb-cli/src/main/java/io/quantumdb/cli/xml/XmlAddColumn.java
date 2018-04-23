package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlAddColumn implements XmlOperation<AddColumn> {

	static final String TAG = "addColumn";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlAddColumn operation = new XmlAddColumn();
		operation.setTableName(element.getAttributes().get("tableName"));

		for (XmlElement child : element.getChildren()) {
			if (child.getTag().equals("column")) {
				operation.setColumn(XmlColumn.convert(child));
			}
		}

		return operation;
	}

	private String tableName;
	private XmlColumn column;

	@Override
	public AddColumn toOperation() {
		return SchemaOperations.addColumn(tableName, column.getName(), null, new Hint[] {});
	}

}
