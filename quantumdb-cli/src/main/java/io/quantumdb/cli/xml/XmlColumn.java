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
		column.setNullable(!Boolean.FALSE.toString().equals(element.getAttributes().get("nullable")));
		if (element.getAttributes().get("unique") != null) {
			column.setUnique(!Boolean.FALSE.toString().equals(element.getAttributes().get("unique")));
			if (column.isUnique() && !Boolean.TRUE.toString().equals(element.getAttributes().get("unique"))) {
				// If unique is not true or false, set composite unique
				column.setCompositeUnique(element.getAttributes().get("unique"));
			} else {
				column.setCompositeUnique(null);
			}
		} else {
			column.setUnique(false);
			column.setCompositeUnique(null);
		}
		return column;
	}

	private String name;
	private String type;
	private String defaultExpression;
	private String compositeUnique;
	private boolean primaryKey;
	private boolean autoIncrement;
	private boolean unique;
	private boolean nullable;

}
