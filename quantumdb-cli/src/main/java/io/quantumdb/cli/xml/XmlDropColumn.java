package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlDropColumn implements XmlOperation<DropColumn> {

	static final String TAG = "dropColumn";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlDropColumn operation = new XmlDropColumn();
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
	public DropColumn toOperation() {
		return SchemaOperations.dropColumn(tableName, columnName);
	}

}
