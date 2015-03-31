package io.quantumdb.core.schema.definitions;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode(of = { "values" })
public class Identity {

	private final Map<String, Object> values;

	public Identity() {
		this.values = Maps.newLinkedHashMap();
	}

	public Identity(String key, Object value) {
		this();
		add(key, value);
	}

	public Identity add(String key, Object value) {
		values.put(key, value);
		return this;
	}

	public Set<String> keys() {
		return values.keySet();
	}

	public Object getValue(String key) {
		return values.get(key);
	}


}
