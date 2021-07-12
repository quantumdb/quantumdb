package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.DropIndex;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlDropIndex implements XmlOperation<DropIndex> {

	static final String TAG = "dropIndex";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlDropIndex operation = new XmlDropIndex();
		operation.setTableName(element.getAttributes().remove("tableName"));
		operation.setColumnNames(element.getAttributes().remove("columnNames").split(","));

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String tableName;
	private String[] columnNames;

	@Override
	public DropIndex toOperation() {
		return SchemaOperations.dropIndex(tableName, columnNames);
	}

}
