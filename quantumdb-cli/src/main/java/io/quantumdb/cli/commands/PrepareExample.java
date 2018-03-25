package io.quantumdb.cli.commands;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.text;
import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.addForeignKey;
import static io.quantumdb.core.schema.operations.SchemaOperations.createTable;

import java.io.IOException;
import java.util.List;

import io.quantumdb.cli.utils.CliException;
import io.quantumdb.cli.utils.CliWriter;
import io.quantumdb.cli.utils.CliWriter.Context;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.schema.definitions.ForeignKey.Action;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrepareExample extends Command {

	@Override
	public Identifier getIdentifier() {
		return new Identifier("prepare-example", "Creates an example changelog.");
	}

	public void perform(CliWriter writer, List<String> arguments) {
		try {
			Config config = Config.load();
			Backend backend = config.getBackend();

			State state = loadState(backend);
			Changelog changelog = state.getChangelog();

			changelog.addChangeSet("Michael de Jong", "Create a users table",
					createTable("users")
							.with("id", bigint(), IDENTITY, AUTO_INCREMENT)
							.with("first_name", text(), NOT_NULL)
							.with("last_name", text(), NOT_NULL));

			changelog.addChangeSet("Michael de Jong", "Create a messages table with foreign keys",
					createTable("messages")
							.with("id", bigint(), IDENTITY, AUTO_INCREMENT)
							.with("sender_id", bigint(), NOT_NULL)
							.with("receiver_id", bigint(), NOT_NULL),

					addForeignKey("messages", "sender_id")
							.referencing("users", "id")
							.named("message_author_fk")
							.onDelete(Action.CASCADE),

					addForeignKey("messages", "receiver_id")
							.referencing("users", "id")
							.named("message_recipient_fk")
							.onDelete(Action.CASCADE));

			changelog.addChangeSet("Michael de Jong", "Add e-mail column to users table",
					addColumn("users", "email", text()));

			persistChanges(backend, state);
		}
		catch (IOException | CliException e) {
			writer.write(e.getMessage(), Context.FAILURE);
		}
	}

}
