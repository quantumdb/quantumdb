package io.quantumdb.core.versioning;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;


/**
 * This class describes a list of functions and triggers which together are used to perform the migration of data
 * from one schema version to another.
 */
public class MigrationFunctions {

	private final Table<String, String, String> functions = HashBasedTable.create();
	private final Table<String, String, String> triggers = HashBasedTable.create();

	public String getFunction(String sourceTableId, String targetTableId) {
		return functions.get(sourceTableId, targetTableId);
	}

	public Table<String, String, String> getFunctions(String tableId) {
		Table<String, String, String> results = HashBasedTable.create();
		functions.column(tableId).forEach((source, name) -> results.put(source, tableId, name));
		functions.row(tableId).forEach((target, name) -> results.put(tableId, target, name));
		return results;
	}

	public String getTrigger(String sourceTableId, String targetTableId) {
		return triggers.get(sourceTableId, targetTableId);
	}

	public Table<String, String, String> getTriggers(String tableId) {
		Table<String, String, String> results = HashBasedTable.create();
		triggers.column(tableId).forEach((source, name) -> results.put(source, tableId, name));
		triggers.row(tableId).forEach((target, name) -> results.put(tableId, target, name));
		return results;
	}

	public Set<Entry<String, String>> getFunctionKeys() {
		return functions.cellSet().stream()
				.map(cell -> new SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
				.collect(Collectors.toSet());
	}

	public Set<Entry<String, String>> getTriggerKeys() {
		return triggers.cellSet().stream()
				.map(cell -> new SimpleEntry<>(cell.getRowKey(), cell.getColumnKey()))
				.collect(Collectors.toSet());
	}

	void add(String sourceTableId, String targetTableId, String triggerName, String functionName) {
		addFunction(sourceTableId, targetTableId, functionName);
		addTrigger(sourceTableId, targetTableId, triggerName);
	}

	void addTrigger(String sourceTableId, String targetTableId, String triggerName) {
		triggers.put(sourceTableId, targetTableId, triggerName);
	}

	void addFunction(String sourceTableId, String targetTableId, String functionName) {
		functions.put(sourceTableId, targetTableId, functionName);
	}

	public void putFunction(String sourceTableId, String targetTableId, String functionName) {
		addFunction(sourceTableId, targetTableId, functionName);
		onFunctionPut(sourceTableId, targetTableId, functionName);
	}

	public void putTrigger(String sourceTableId, String targetTableId, String triggerName) {
		addTrigger(sourceTableId, targetTableId, triggerName);
		onTriggerPut(sourceTableId, targetTableId, triggerName);
	}

	public void remove(String sourceTableId, String targetTableId) {
		removeFunction(sourceTableId, targetTableId);
		removeTrigger(sourceTableId, targetTableId);
	}

	public void removeTrigger(String sourceTableId, String targetTableId) {
		triggers.remove(sourceTableId, targetTableId);
		onTriggerRemove(sourceTableId, targetTableId);
	}

	public void removeFunction(String sourceTableId, String targetTableId) {
		functions.remove(sourceTableId, targetTableId);
		onFunctionRemove(sourceTableId, targetTableId);
	}

	protected void onFunctionPut(String sourceTableId, String targetTableId, String functionName) {
		// Allow for overriding...
	}

	protected void onFunctionRemove(String sourceTableId, String targetTableId) {
		// Allow for overriding...
	}

	protected void onTriggerPut(String sourceTableId, String targetTableId, String triggerName) {
		// Allow for overriding...
	}

	protected void onTriggerRemove(String sourceTableId, String targetTableId) {
		// Allow for overriding...
	}

}
