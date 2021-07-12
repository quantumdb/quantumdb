package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Optional;

import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.PostgresTypes;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlAlterColumn implements XmlOperation<AlterColumn> {

	static final String TAG = "alterColumn";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlAlterColumn operation = new XmlAlterColumn();
		operation.setTableName(element.getAttributes().remove("tableName"));
		operation.setColumnName(element.getAttributes().remove("columnName"));

		Optional.ofNullable(element.getAttributes().remove("newColumnName"))
				.ifPresent(operation::setNewColumnName);

		Optional.ofNullable(element.getAttributes().remove("newDataType"))
				.ifPresent(operation::setNewType);

		Optional.ofNullable(element.getAttributes().remove("nullable"))
				.map(Boolean.TRUE.toString()::equals)
				.ifPresent(operation::setNullable);

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return operation;
	}

	private String tableName;
	private String columnName;

	private String newColumnName;
	private String newType;
	private Boolean nullable;

	@Override
	public AlterColumn toOperation() {
		AlterColumn operation = SchemaOperations.alterColumn(tableName, columnName);
		Optional.ofNullable(newColumnName).ifPresent(operation::rename);
		Optional.ofNullable(newType).map(PostgresTypes::from).ifPresent(operation::modifyDataType);

		Optional.ofNullable(nullable).ifPresent(newNullable -> {
			if (newNullable) {
				operation.dropHint(Hint.NOT_NULL);
			}
			else {
				operation.addHint(Hint.NOT_NULL);
			}
		});

		// TODO: Support other hints?

		return operation;
	}

}
