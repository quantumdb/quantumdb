package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlRenameTable implements XmlOperation<RenameTable> {

	static final String TAG = "renameTable";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlRenameTable operation = new XmlRenameTable();
		operation.setOldTableName(element.getAttributes().get("oldTableName"));
		operation.setNewTableName(element.getAttributes().get("newTableName"));
		return operation;
	}

	private String oldTableName;
	private String newTableName;

	@Override
	public RenameTable toOperation() {
		return SchemaOperations.renameTable(oldTableName, newTableName);
	}

}
