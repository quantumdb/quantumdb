package io.quantumdb.cli.commands;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.versioning.ChangeSet;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Changelog extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("changelog", "Lists changes recorded in the changelog.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			String from = getArgument(arguments, "from", String.class, null);
			String until = getArgument(arguments, "until", String.class, null);
			Integer limit = getArgument(arguments, "limit", Integer.class, () -> Integer.MAX_VALUE);
			boolean printShort = getArgument(arguments, "short", Boolean.class, () -> false);

			Config config = Config.load();
			Backend backend = config.getBackend();

			State state = loadState(backend);
			Set<Version> activeVersions = Sets.newHashSet(state.getRefLog().getVersions());
			io.quantumdb.core.versioning.Changelog changelog = state.getChangelog();

			ChangeSet changeset = null;
			List<Version> versions = Lists.newArrayList(changelog.getRoot());

			boolean print = from == null;
			while (!versions.isEmpty()) {
				Version version = versions.remove(0);
				ChangeSet currentChangeset = version.getChangeSet();

				if (version.getId().equals(from)) {
					print = true;
				}

				if (!currentChangeset.equals(changeset) && print) {
					print(writer, currentChangeset, activeVersions, printShort);
					limit--;
				}

				if (version.getId().equals(until) || limit <= 0) {
					print = false;
				}

				changeset = currentChangeset;
				if (version.getChild() != null) {
					versions.add(version.getChild());
				}
			}

			persistChanges(backend, state);
		}
		catch (IOException | CliException e) {
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

	private void print(CliWriter writer, ChangeSet changeSet, Set<Version> activeVersions, boolean printShort) {
		Version pointer = changeSet.getVersion();
		Version lastVersion = pointer;
		boolean active = activeVersions.contains(pointer);

		int operations = 0;
		while (pointer != null && changeSet.equals(pointer.getChangeSet())) {
			if (pointer.getOperation() != null) {
				operations++;
			}
			pointer = pointer.getParent();
		}

		String id = lastVersion.getId();
		if (active) {
			id += " (active)";
		}
		id += " - " + changeSet.getId();

		writer.setIndent(0);
		writer.write(id, Context.SUCCESS);

		if (!printShort) {
			writer.indent(1);
			writer.write("Date: " + changeSet.getCreated(), Context.INFO);

			if (!Strings.isNullOrEmpty(changeSet.getAuthor())) {
				writer.write("Author: " + changeSet.getAuthor(), Context.INFO);
			}

			if (operations > 0) {
				writer.write("Operations: " + operations, Context.INFO);
			}

			if (!Strings.isNullOrEmpty(changeSet.getDescription())) {
				writer.write("Description: " + changeSet.getDescription(), Context.INFO);
			}

			writer.newLine();
		}
	}

}
