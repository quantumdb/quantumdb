package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlDropTable implements XmlOperation<DropTable> {

	static final String TAG = "dropTable";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlDropTable operation = new XmlDropTable();
		operation.setTableName(element.getAttributes().remove("tableName"));

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String tableName;

	@Override
	public DropTable toOperation() {
		return SchemaOperations.dropTable(tableName);
	}

}
