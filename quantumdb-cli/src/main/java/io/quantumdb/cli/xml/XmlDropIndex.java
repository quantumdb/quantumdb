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
		operation.setTableName(element.getAttributes().get("tableName"));
		operation.setColumnNames(element.getAttributes().get("columnNames").split(","));
		return operation;
	}

	private String tableName;
	private String[] columnNames;

	@Override
	public DropIndex toOperation() {
		return SchemaOperations.dropIndex(tableName, columnNames);
	}

}
