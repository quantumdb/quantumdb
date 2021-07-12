package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Optional;

import io.quantumdb.core.schema.operations.CreateIndex;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlCreateIndex implements XmlOperation<CreateIndex> {

	static final String TAG = "createIndex";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlCreateIndex operation = new XmlCreateIndex();
		operation.setTableName(element.getAttributes().remove("tableName"));
		operation.setColumnNames(element.getAttributes().remove("columnNames").split(","));

		Optional.ofNullable(element.getAttributes().remove("unique"))
				.map(Boolean.TRUE.toString()::equals)
				.ifPresent(operation::setUnique);

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String tableName;
	private String[] columnNames;
	private boolean unique;

	@Override
	public CreateIndex toOperation() {
		return SchemaOperations.createIndex(tableName, unique, columnNames);
	}

}
