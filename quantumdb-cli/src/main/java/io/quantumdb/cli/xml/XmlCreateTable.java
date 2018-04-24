package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.PostgresTypes;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlCreateTable implements XmlOperation<CreateTable> {

	static final String TAG = "createTable";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlCreateTable operation = new XmlCreateTable();
		operation.setTableName(element.getAttributes().get("tableName"));

		for (XmlElement child : element.getChildren()) {
			if (child.getTag().equals("columns")) {
				for (XmlElement subChild : child.getChildren()) {
					operation.getColumns().add(XmlColumn.convert(subChild));
				}
			}
		}

		return operation;
	}

	private String tableName;
	private final List<XmlColumn> columns = Lists.newArrayList();

	@Override
	public CreateTable toOperation() {
		CreateTable operation = SchemaOperations.createTable(tableName);
		for (XmlColumn column : columns) {
			ColumnType type = PostgresTypes.from(column.getType());
			String defaultExpression = column.getDefaultExpression();

			List<Hint> hints = Lists.newArrayList();
			if (column.isPrimaryKey()) {
				hints.add(Hint.IDENTITY);
			}
			if (column.isAutoIncrement()) {
				hints.add(Hint.AUTO_INCREMENT);
			}
			if (!column.isNullable()) {
				hints.add(Hint.NOT_NULL);
			}

			Hint[] hintArray = hints.toArray(new Hint[hints.size()]);
			operation.with(column.getName(), type, defaultExpression, hintArray);
		}
		return operation;
	}

}
