package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import lombok.Data;

@Data
public class XmlColumn {

	static XmlColumn convert(XmlElement element) {
		checkArgument(element.getTag().equals("column"));

		XmlColumn column = new XmlColumn();
		column.setName(element.getAttributes().get("name"));
		column.setType(element.getAttributes().get("type"));
		column.setDefaultExpression(element.getAttributes().get("defaultExpression"));
		column.setPrimaryKey(Boolean.TRUE.toString().equals(element.getAttributes().get("primaryKey")));
		column.setAutoIncrement(Boolean.TRUE.toString().equals(element.getAttributes().get("autoIncrement")));
		column.setNullable(Boolean.TRUE.toString().equals(element.getAttributes().get("nullable")));
		return column;
	}

	private String name;
	private String type;
	private String defaultExpression;
	private boolean primaryKey;
	private boolean autoIncrement;
	private boolean nullable;

}
