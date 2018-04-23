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
		operation.setTableName(element.getAttributes().get("tableName"));
		operation.setColumnNames(element.getAttributes().get("columnNames").split(","));

		Optional.ofNullable(element.getAttributes().get("unique"))
				.map(Boolean.TRUE.toString()::equals)
				.ifPresent(operation::setUnique);

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
