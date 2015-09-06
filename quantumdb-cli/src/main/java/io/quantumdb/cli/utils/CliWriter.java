package io.quantumdb.cli.utils;

import java.io.IOException;

import com.google.common.base.Strings;
import jline.ANSIBuffer;
import jline.ConsoleReader;
import jline.Terminal;
import lombok.Data;
import lombok.SneakyThrows;

@Data
public class CliWriter {

	public enum Context {
		INFO, SUCCESS, FAILURE;
	}

	private final Terminal terminal;
	private final ConsoleReader reader;
	private int indent = 0;

	public CliWriter() {
		try {
			terminal = Terminal.setupTerminal();
			reader = new ConsoleReader();
			terminal.beforeReadLine(reader, "", (char) 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public void close() {
		Terminal.resetTerminal();
	}

	public CliWriter indent(int delta) {
		indent += delta;
		return this;
	}

	public CliWriter setIndent(int indent) {
		this.indent = indent;
		return this;
	}

	@SneakyThrows(IOException.class)
	public CliWriter newLine() {
		reader.printNewline();
		return this;
	}

	public CliWriter write(String message) {
		return write(message, Context.INFO);
	}

	@SneakyThrows(IOException.class)
	public CliWriter write(String message, Context context) {
		String marker = "";
		if (indent == 0) {
			marker = "==> ";
		}
		if (indent > 0) {
			marker = "  > ";
		}

		if (indent >= 0) {
			message = Strings.repeat("  ", indent) + marker + message;
		}
		if (indent <= 0) {
			message = new ANSIBuffer().bold(message).toString();
		}

		switch (context) {
			case INFO:
				reader.printString(new ANSIBuffer().append(message).toString());
				break;
			case SUCCESS:
				reader.printString(new ANSIBuffer().green(message).toString());
				break;
			case FAILURE:
				reader.printString(new ANSIBuffer().red(message).toString());
				break;
		}
		reader.printNewline();
		return this;
	}

}
