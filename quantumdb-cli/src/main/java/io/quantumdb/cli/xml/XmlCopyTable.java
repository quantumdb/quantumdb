package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.CopyTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlCopyTable implements XmlOperation<CopyTable> {

	static final String TAG = "copyTable";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlCopyTable operation = new XmlCopyTable();
		operation.setSourceTableName(element.getAttributes().remove("sourceTableName"));
		operation.setTargetTableName(element.getAttributes().remove("targetTableName"));

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String sourceTableName;
	private String targetTableName;

	@Override
	public CopyTable toOperation() {
		return SchemaOperations.copyTable(sourceTableName, targetTableName);
	}

}
