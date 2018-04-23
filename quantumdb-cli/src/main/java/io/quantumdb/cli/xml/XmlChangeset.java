package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import lombok.Data;

@Data
public class XmlChangeset {

	static XmlChangeset convert(XmlElement element) {
		checkArgument(element.getTag().equals("changeset"));

		XmlChangeset changeset = new XmlChangeset();
		changeset.setId(element.getAttributes().get("id"));
		changeset.setAuthor(element.getAttributes().get("author"));

		for (XmlElement child : element.getChildren()) {
			if (child.getTag().equals("operations")) {
				for (XmlElement subChild : child.getChildren()) {
					changeset.getOperations().add(XmlOperation.convert(subChild));
				}
			}
			else if (child.getTag().equals("description")) {
				String description = child.getChildren().stream()
						.filter(subChild -> subChild.getTag() == null)
						.filter(subChild -> subChild.getText() != null)
						.map(XmlElement::getText)
						.collect(Collectors.joining());

				changeset.setDescription(description);
			}
		}

		return changeset;
	}

	private String id;
	private String author;
	private String description;
	private final List<XmlOperation<?>> operations = Lists.newArrayList();

}
