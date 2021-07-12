package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlAlterDefaultExpression implements XmlOperation<AlterColumn> {

	static final String TAG = "alterDefaultExpression";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlAlterDefaultExpression operation = new XmlAlterDefaultExpression();
		operation.setTableName(element.getAttributes().remove("tableName"));
		operation.setColumnName(element.getAttributes().remove("columnName"));
		operation.setDefaultExpression(element.getAttributes().remove("defaultExpression"));

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String tableName;
	private String columnName;
	private String defaultExpression;

	@Override
	public AlterColumn toOperation() {
		return SchemaOperations.alterColumn(tableName, columnName)
				.modifyDefaultExpression(defaultExpression);
	}

}
