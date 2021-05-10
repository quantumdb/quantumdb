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

		Optional.ofNullable(attributes.get("unique"))
				.map(Boolean.TRUE.toString()::equals)
				.ifPresent(operation::setUnique);

		Optional.ofNullable(attributes.get("primaryKey"))
				.map(Boolean.TRUE.toString()::equals)
				.ifPresent(operation::setPrimaryKey);

		Optional.ofNullable(attributes.get("autoIncrementing"))
				.map(Boolean.TRUE.toString()::equals)
				.ifPresent(operation::setAutoIncrementing);

		return operation;
	}

	private String tableName;
	private String columnName;

	private String newColumnName;
	private String newType;
	private Boolean nullable;
	private Boolean unique;
	private Boolean primaryKey;
	private Boolean autoIncrementing;

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

		// TODO: NOT REALLY TESTED!

		Optional.ofNullable(unique).ifPresent(newUnique -> {
			if (newUnique) {
				operation.addHint(Hint.UNIQUE);
			}
			else {
				operation.dropHint(Hint.UNIQUE);
			}
		});

		Optional.ofNullable(primaryKey).ifPresent(newPrimarykey -> {
			if (newPrimarykey) {
				operation.addHint(Hint.PRIMARY_KEY);
			}
			else {
				operation.dropHint(Hint.PRIMARY_KEY);
			}
		});

		Optional.ofNullable(autoIncrementing).ifPresent(newAutoIncrementing -> {
			if (newAutoIncrementing) {
				operation.addHint(Hint.AUTO_INCREMENT);
			}
			else {
				operation.dropHint(Hint.AUTO_INCREMENT);
			}
		});

		return operation;
	}

}
