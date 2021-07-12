package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlDropDefaultExpression implements XmlOperation<AlterColumn> {

	static final String TAG = "dropDefaultExpression";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlDropDefaultExpression operation = new XmlDropDefaultExpression();
		operation.setTableName(element.getAttributes().remove("tableName"));
		operation.setColumnName(element.getAttributes().remove("columnName"));

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String tableName;
	private String columnName;

	@Override
	public AlterColumn toOperation() {
		return SchemaOperations.alterColumn(tableName, columnName)
				.dropDefaultExpression();
	}

}
