package io.quantumdb.cli.xml;

import io.quantumdb.core.schema.operations.Operation;

public interface XmlOperation<T extends Operation> {

	static XmlOperation<?> convert(XmlElement element) {
		switch (element.getTag()) {
			case XmlCreateTable.TAG:
				return XmlCreateTable.convert(element);
			case XmlDropTable.TAG:
				return XmlDropTable.convert(element);
			case XmlRenameTable.TAG:
				return XmlRenameTable.convert(element);
			case XmlCopyTable.TAG:
				return XmlCopyTable.convert(element);
			case XmlAddColumn.TAG:
				return XmlAddColumn.convert(element);
			case XmlAlterColumn.TAG:
				return XmlAlterColumn.convert(element);
			case XmlAlterDefaultExpression.TAG:
				return XmlAlterDefaultExpression.convert(element);
			case XmlDropDefaultExpression.TAG:
				return XmlDropDefaultExpression.convert(element);
			case XmlDropColumn.TAG:
				return XmlDropColumn.convert(element);
			case XmlAddForeignKey.TAG:
				return XmlAddForeignKey.convert(element);
			case XmlDropForeignKey.TAG:
				return XmlDropForeignKey.convert(element);
			case XmlCreateIndex.TAG:
				return XmlCreateIndex.convert(element);
			case XmlDropIndex.TAG:
				return XmlDropIndex.convert(element);
			case XmlQuery.TAG:
				return XmlQuery.convert(element);
			default:
				throw new IllegalArgumentException("Unknown type of operation: " + element.getTag());
		}
	}

	T toOperation();
}
