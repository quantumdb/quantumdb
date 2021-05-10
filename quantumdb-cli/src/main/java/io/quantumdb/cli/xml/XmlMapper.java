package io.quantumdb.cli.xml;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class XmlMapper {

	public XmlChangelog loadChangelog(String file) throws IOException {
		XmlElement element = load(file);
		return XmlChangelog.convert(element);
	}

	/**
	 * Parses the current XML file into XmlElements and gives back the root XmlElement
	 * @param file the file the parser should load
	 * @return root XmlElement of XML file
	 * @throws IOException when XML file is not correct
	 */
	private XmlElement load(String file) throws IOException {
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();

			AtomicReference<XmlElement> result = new AtomicReference<>();
			List<XmlElement> stack = Lists.newArrayList();
			DefaultHandler handler = new DefaultHandler() {
				@SneakyThrows
				public void startElement(String uri, String localName, String qName, Attributes attributes) {
					XmlElement element = new XmlElement(qName, null);
					for (int i = 0; i < attributes.getLength(); i++) {
						String key = attributes.getQName(i);
						String value = attributes.getValue(i);
						// Convert all values to lowercase except when quoted
						// PostgreSQL converts table and column names to lowercase while Oracle converts them to uppercase
						// XML double quote escapement is &quot;
						char[] chars = value.toCharArray();
						boolean insideQuotes = false;
						for (int j = 0; j < chars.length; j++) {
							if (chars[j] == '"') {
								insideQuotes = !insideQuotes;
							} else if (!insideQuotes) {
								chars[j] = Character.toLowerCase(chars[j]);
							}
						}
						value = String.valueOf(chars);
						element.getAttributes().put(key, value);
					}

					if (stack.size() > 0) {
						XmlElement parent = stack.get(stack.size() - 1);
						parent.getChildren().add(element);
					}

					stack.add(element);
				}

				@SneakyThrows
				public void endElement(String uri, String localName, String qName) {
					XmlElement removed;
					do {
						removed = stack.remove(stack.size() - 1);
					}
					while (removed.getTag() == null);

					if (!removed.getTag().equals(qName)) {
						throw new SAXException("Unexpected closing tag: " + qName);
					}

					if (stack.isEmpty()) {
						result.set(removed);
					}
				}

				@SneakyThrows
				public void characters(char[] ch, int start, int length) {
					String body = new String(ch, start, length);
					if (body.trim().isEmpty()) {
						return;
					}

					XmlElement element = new XmlElement(null, body);

					if (stack.size() > 0) {
						XmlElement parent = stack.get(stack.size() - 1);
						parent.getChildren().add(element);
					}

					stack.add(element);
				}
			};

			saxParser.parse(new File(file), handler);
			return result.get();
		}
		catch (ParserConfigurationException | SAXException e) {
			throw new RuntimeException(e);
		}
	}

}
