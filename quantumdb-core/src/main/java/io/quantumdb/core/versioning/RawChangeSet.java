package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

import java.io.Serializable;
import java.util.Date;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@Table(name = "quantumdb_changesets")
public class RawChangeSet implements Serializable {

	@Id
	@Column(name = "version_id")
	private String versionId;

	@OneToOne(cascade = CascadeType.ALL)
	@PrimaryKeyJoinColumn
	private RawVersion version;

	@Column(name = "author")
	private String author;

	@Column(name = "created")
	private Date created;

	@Column(name = "description")
	private String description;

	RawChangeSet(RawVersion version, String author, Date created, String description) {
		checkArgument(version != null, "You must specify a 'version'.");
		checkArgument(!isNullOrEmpty(author), "You must specify an 'author'.");
		checkArgument(created != null, "You must specify a 'created' Date.");

		this.version = version;
		this.author = author;
		this.created = created;
		this.description = emptyToNull(description);
	}

}
