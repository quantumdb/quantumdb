package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.PostgresTypes;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlAddColumn implements XmlOperation<AddColumn> {

	static final String TAG = "addColumn";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlAddColumn operation = new XmlAddColumn();
		operation.setTableName(element.getAttributes().get("tableName"));

		for (XmlElement child : element.getChildren()) {
			if (child.getTag().equals("column")) {
				operation.setColumn(XmlColumn.convert(child));
			}
		}

		return operation;
	}

	private String tableName;
	private XmlColumn column;

	@Override
	public AddColumn toOperation() {
		ColumnType dataType = PostgresTypes.from(column.getType());
		String columnName = column.getName();
		String defaultExpression = column.getDefaultExpression();

		List<Hint> hints = Lists.newArrayList();
		if (column.isNullable()) {
			hints.add(Hint.NOT_NULL);
		}
		if (column.isAutoIncrement()) {
			hints.add(Hint.AUTO_INCREMENT);
		}
		if (column.isPrimaryKey()) {
			hints.add(Hint.IDENTITY);
		}

		Hint[] hintArray = hints.toArray(new Hint[hints.size()]);
		return SchemaOperations.addColumn(tableName, columnName, dataType, defaultExpression, hintArray);
	}

}
