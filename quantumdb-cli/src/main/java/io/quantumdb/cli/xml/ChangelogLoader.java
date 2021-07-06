package io.quantumdb.cli.xml;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ChangelogLoader {

	public Changelog load(Changelog changelog, String file) throws IOException {
		XmlChangelog xml = new XmlMapper().loadChangelog(file);
		List<XmlChangeset> changesets = xml.getChangesets();

		HashSet<String> idSet = new HashSet<>();
		Version temp = changelog.getRoot();
		idSet.add(temp.getId());
		while (temp.getChild() != null) {
			temp = temp.getChild();
			if (idSet.contains(temp.getId())) {
				throw new IllegalStateException("Existing changelog in the database already has two id's with the name: " + temp.getId() + ", please change one id in the database.");
			}
			idSet.add(temp.getId());
		}

		for (XmlChangeset changeset : changesets) {
			if (idSet.contains(changeset.getId())) {
				throw new IllegalStateException("Changelog must contain only unique id's, id: " + changeset.getId() + " is already present.");
			}
			else {
				idSet.add(changeset.getId());
			}
		}

		Version pointer = changelog.getRoot().getChild();
		for (XmlChangeset changeset : changesets) {
			String changesetId = changeset.getId();
			if (pointer != null) {
				for (int index = 1; index <= changeset.getOperations().size(); index++) {
					XmlOperation<?> xmlOperation = changeset.getOperations().get(index - 1);
					Operation operation = xmlOperation.toOperation();
					Operation currentOperation = pointer.getOperation();

					if (!operation.equals(currentOperation)) {
						throw new IllegalStateException("Operation at index: " + index + " in changeset: "
								+ changesetId + " differs from the one already defined in the database.");
					}
					if (!changesetId.equals(pointer.getChangeSet().getId())) {
						throw new IllegalStateException("The changeset with id: " + changesetId + " was " +
								"unexpected as the database already contains a different changeset in that position: "
								+ pointer.getChangeSet().getId());
					}

					pointer = pointer.getChild();
					if (pointer == null) {
						break;
					}
				}
			}
			else {
				String author = changeset.getAuthor();
				String description = changeset.getDescription();
				List<Operation> operations = changeset.getOperations().stream()
						.map(XmlOperation::toOperation)
						.collect(Collectors.toList());

				changelog.addChangeSet(changesetId, author, description, operations);
			}
		}

		return changelog;
	}

}
