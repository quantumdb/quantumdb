package io.quantumdb.cli.xml;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

@Data
class XmlElement {

	private final String tag;
	private final Map<String, String> attributes = Maps.newHashMap();
	private final List<XmlElement> children = Lists.newArrayList();
	private final String text;

}