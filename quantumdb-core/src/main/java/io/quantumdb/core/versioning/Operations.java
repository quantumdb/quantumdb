package io.quantumdb.core.versioning;

import java.util.Optional;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.CopyTable;
import io.quantumdb.core.schema.operations.CreateIndex;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.DataOperation;
import io.quantumdb.core.schema.operations.DecomposeTable;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.schema.operations.DropIndex;
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.schema.operations.JoinTable;
import io.quantumdb.core.schema.operations.MergeTable;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.schema.operations.PartitionTable;
import io.quantumdb.core.schema.operations.RenameTable;

class Operations {

	private final BiMap<String, Class<? extends Operation>> mapping = HashBiMap.create();

	Operations() {
		mapping.put("add-column", AddColumn.class);
		mapping.put("add-foreign-key", AddForeignKey.class);
		mapping.put("alter-column", AlterColumn.class);
		mapping.put("copy-table", CopyTable.class);
		mapping.put("create-index", CreateIndex.class);
		mapping.put("create-table", CreateTable.class);
		mapping.put("dml-query", DataOperation.class);
		mapping.put("decompose-table", DecomposeTable.class);
		mapping.put("drop-column", DropColumn.class);
		mapping.put("drop-foreign-key", DropForeignKey.class);
		mapping.put("drop-index", DropIndex.class);
		mapping.put("drop-table", DropTable.class);
		mapping.put("join-table", JoinTable.class);
		mapping.put("merge-table", MergeTable.class);
		mapping.put("partition-table", PartitionTable.class);
		mapping.put("rename-table", RenameTable.class);
	}

	public Optional<Class<? extends Operation>> getOperationType(String operationType) {
		return Optional.ofNullable(mapping.get(operationType));
	}

	public Optional<String> getOperationType(Class<? extends Operation> operationType) {
		return Optional.ofNullable(mapping.inverse().get(operationType));
	}

}
