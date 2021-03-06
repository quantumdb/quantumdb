package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlDropForeignKey implements XmlOperation<DropForeignKey> {

	static final String TAG = "dropForeignKey";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlDropForeignKey operation = new XmlDropForeignKey();
		operation.setTableName(element.getAttributes().remove("tableName"));
		operation.setForeignKeyName(element.getAttributes().remove("foreignKeyName"));

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String tableName;
	private String foreignKeyName;

	@Override
	public DropForeignKey toOperation() {
		return SchemaOperations.dropForeignKey(tableName, foreignKeyName);
	}

}
