package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import lombok.Data;

@Data
public class XmlColumn {

	static XmlColumn convert(XmlElement element) {
		checkArgument(element.getTag().equals("column"));

		XmlColumn column = new XmlColumn();
		column.setName(element.getAttributes().remove("name"));
		column.setType(element.getAttributes().remove("type"));
		column.setDefaultExpression(element.getAttributes().remove("defaultExpression"));
		column.setPrimaryKey(Boolean.TRUE.toString().equals(element.getAttributes().remove("primaryKey")));
		column.setAutoIncrement(Boolean.TRUE.toString().equals(element.getAttributes().remove("autoIncrement")));
		column.setNullable(!Boolean.FALSE.toString().equals(element.getAttributes().remove("nullable")));

		if (!element.getAttributes().keySet().isEmpty()) {
			throw new IllegalArgumentException("Attributes: " + element.getAttributes().keySet() + " is/are not valid!");
		}

		return column;
	}

	private String name;
	private String type;
	private String defaultExpression;
	private boolean primaryKey;
	private boolean autoIncrement;
	private boolean nullable;

}
