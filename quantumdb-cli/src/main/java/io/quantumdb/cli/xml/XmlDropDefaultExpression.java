package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlDropDefaultExpression implements XmlOperation<AlterColumn> {

	static final String TAG = "dropDefaultExpression";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));
		Map<String, String> attributes = element.getAttributes();

		XmlDropDefaultExpression operation = new XmlDropDefaultExpression();
		operation.setTableName(attributes.get("tableName"));
		operation.setColumnName(attributes.get("columnName"));
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
