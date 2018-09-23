package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Objects;
import java.util.Optional;

import io.quantumdb.core.schema.operations.DataOperation;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;

@Data
public class XmlQuery implements XmlOperation<DataOperation> {

	static final String TAG = "sql";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));

		XmlQuery operation = new XmlQuery();
		operation.setQuery(Optional.ofNullable(element.getText())
				.orElseGet(() -> element.getChildren().stream()
						.filter(child -> child.getTag() == null)
						.map(XmlElement::getText)
						.filter(Objects::nonNull)
						.findFirst()
						.orElseThrow(() -> new RuntimeException("No query specified!"))));

		return operation;
	}

	private String query;

	@Override
	public DataOperation toOperation() {
		return SchemaOperations.execute(query);
	}

}
