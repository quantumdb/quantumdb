package io.quantumdb.cli.commands;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.versioning.ChangeSet;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

@Slf4j
public class Changelog extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("changelog", "Lists changes recorded in the changelog.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			Backend backend = config.getBackend();

			State state = loadState(backend);
			Set<Version> activeVersions = Sets.newHashSet(state.getTableMapping().getVersions());
			io.quantumdb.core.versioning.Changelog changelog = state.getChangelog();

			ChangeSet changeSet = null;
			List<Version> versions = Lists.newArrayList(changelog.getRoot());

			while (!versions.isEmpty()) {
				Version version = versions.remove(0);
				print(writer, changeSet, version, activeVersions);
				changeSet = version.getChangeSet();

				if (version.getChild() != null) {
					versions.add(version.getChild());
				}
			}

			for (Version version : activeVersions) {
				print(writer, changeSet, version, activeVersions);
			}
		}
		catch (IOException | CliException e) {
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

	private void print(CliWriter writer, ChangeSet previousChangeSet, Version current, Set<Version> activeVersions) {
		ChangeSet changeSet = current.getChangeSet();
		boolean sameChangeSet = changeSet.equals(previousChangeSet);

		if (!sameChangeSet) {
			writer.newLine();
			writer.setIndent(0);
			String message = changeSet.getDescription() + " - " + changeSet.getAuthor();
			Version parent = current.getParent();
			while (parent != null && parent.getChangeSet().equals(current.getChangeSet())) {
				parent = parent.getParent();
			}
			if (parent != null) {
				message = message + " (based on: " + parent.getId() + ")";
			}
			writer.write(message, Context.SUCCESS);
		}

		writer.setIndent(1);
		SchemaOperation schemaOperation = current.getSchemaOperation();

		StringBuilder builder = new StringBuilder();
		builder.append(current.getId());
		if (activeVersions.remove(current)) {
			builder.append(" (active)");
		}
		if (schemaOperation != null) {
			builder.append(" - " + StringUtils.abbreviate(schemaOperation.toString(), 60));
		}
		writer.write(builder.toString());
	}

}
