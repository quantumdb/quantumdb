package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
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
		Map<String, String> attributes = element.getAttributes();

		XmlAlterColumn operation = new XmlAlterColumn();
		operation.setTableName(attributes.get("tableName"));
		operation.setColumnName(attributes.get("columnName"));

		Optional.ofNullable(attributes.get("newColumnName"))
				.ifPresent(operation::setNewColumnName);

		Optional.ofNullable(attributes.get("newType"))
				.ifPresent(operation::setNewType);

		Optional.ofNullable(attributes.get("nullable"))
				.map(Boolean.TRUE.toString()::equals)
				.ifPresent(operation::setNullable);

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
