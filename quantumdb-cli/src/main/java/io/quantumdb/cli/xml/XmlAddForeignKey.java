package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Optional;

import io.quantumdb.core.schema.definitions.ForeignKey.Action;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlAddForeignKey implements XmlOperation<AddForeignKey> {

	static final String TAG = "addForeignKey";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlAddForeignKey operation = new XmlAddForeignKey();
		operation.setTableName(element.getAttributes().get("tableName"));
		operation.setColumnNames(element.getAttributes().get("columnNames").split(","));
		operation.setReferencedTableName(element.getAttributes().get("referencesTableName"));
		operation.setReferencedColumnNames(element.getAttributes().get("referencesColumnNames").split(","));

		Optional.ofNullable(element.getAttributes().get("name"))
				.ifPresent(operation::setName);

		Optional.ofNullable(element.getAttributes().get("onDelete"))
				.map(Action::valueOf)
				.ifPresent(operation::setOnDelete);

		Optional.ofNullable(element.getAttributes().get("onUpdate"))
				.map(Action::valueOf)
				.ifPresent(operation::setOnUpdate);

		return operation;
	}

	private String tableName;
	private String[] columnNames;
	private String referencedTableName;
	private String[] referencedColumnNames;

	private String name;
	private Action onDelete;
	private Action onUpdate;

	@Override
	public AddForeignKey toOperation() {
		AddForeignKey operation = SchemaOperations.addForeignKey(tableName, columnNames)
				.referencing(referencedTableName, referencedColumnNames);

		Optional.ofNullable(name).ifPresent(operation::named);
		Optional.ofNullable(onDelete).ifPresent(operation::onDelete);
		Optional.ofNullable(onUpdate).ifPresent(operation::onUpdate);

		return operation;
	}

}
