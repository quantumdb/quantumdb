package io.quantumdb.core.versioning;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@Table(name = "quantumdb_tablemappings")
public class TableNameMapping implements Serializable {

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TableNameMappingId implements Serializable {
		private String tableName;
		private String version;
	}

	@Id
	@Column(name = "table_name")
	private String tableName;

	@Id
	@ManyToOne
	@JoinColumn(name = "version_id")
	private RawVersion version;

	@Column(name = "table_id")
	private String tableId;

}
