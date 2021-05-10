package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.PostgresTypes;
import io.quantumdb.core.schema.definitions.Unique;
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
		HashMap<String, List<String>> uniques = new HashMap<>();
		for (XmlColumn column : columns) {
			ColumnType type = PostgresTypes.from(column.getType());
			String defaultExpression = column.getDefaultExpression();

			List<Hint> hints = Lists.newArrayList();
			if (column.isPrimaryKey()) {
				hints.add(Hint.PRIMARY_KEY);
			}
			if (column.isAutoIncrement()) {
				hints.add(Hint.AUTO_INCREMENT);
			}
			if (!column.isNullable()) {
				hints.add(Hint.NOT_NULL);
			}
			if (column.isUnique()) {
				hints.add(Hint.UNIQUE);
				// If part of existing Unique object add it, otherwise create new Unique object
				if (column.getCompositeUnique() != null) {
					if (uniques.containsKey(column.getCompositeUnique())) {
						uniques.get(column.getCompositeUnique()).add(column.getName());
					} else {
						uniques.put(column.getCompositeUnique(), Lists.newArrayList(column.getName()));
					}
				} else {
					uniques.put(column.getName(), Lists.newArrayList(column.getName()));
				}
			}

			Hint[] hintArray = hints.toArray(new Hint[0]);
			operation.with(column.getName(), type, defaultExpression, hintArray);
		}
		for (String key: uniques.keySet()) {
			Unique unique = new Unique(uniques.get(key));
			operation.withUnique(unique);
		}

		return operation;
	}

}
