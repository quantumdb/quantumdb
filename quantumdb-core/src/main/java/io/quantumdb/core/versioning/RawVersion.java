package io.quantumdb.core.versioning;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import java.io.Serializable;

import com.google.gson.Gson;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.schema.operations.SchemaOperations;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@Table(name = "quantumdb_changelog")
public class RawVersion implements Serializable {

	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "parent_version_id")
	private RawVersion parentVersion;

	@Id
	@Column(name = "version_id")
	private String id;

	@OneToOne(fetch = FetchType.LAZY, mappedBy = "version")
	private RawChangeSet changeSet;

	@Column(name = "schema_operation", nullable = false)
	private String schemaOperation;

	<T extends SchemaOperation> T getSchemaOperation(Gson deserializer, Class<T> type) {
		return deserializer.fromJson(schemaOperation, type);
	}

}
