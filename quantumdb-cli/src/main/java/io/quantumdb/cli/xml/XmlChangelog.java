package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import com.google.common.collect.Lists;
import lombok.Data;

@Data
public class XmlChangelog {

	static XmlChangelog convert(XmlElement element) {
		checkArgument(element.getTag().equals("changelog"));

		XmlChangelog changelog = new XmlChangelog();
		for (XmlElement child : element.getChildren()) {
			changelog.getChangesets().add(XmlChangeset.convert(child));
		}

		return changelog;
	}

	private final List<XmlChangeset> changesets = Lists.newArrayList();

}
