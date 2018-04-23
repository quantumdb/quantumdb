package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlAlterDefaultExpression implements XmlOperation<AlterColumn> {

	static final String TAG = "alterDefaultExpression";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));
		Map<String, String> attributes = element.getAttributes();

		XmlAlterDefaultExpression operation = new XmlAlterDefaultExpression();
		operation.setTableName(attributes.get("tableName"));
		operation.setColumnName(attributes.get("columnName"));
		operation.setDefaultExpression(attributes.get("defaultExpression"));
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
