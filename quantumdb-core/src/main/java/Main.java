import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Strings;
import jline.ANSIBuffer;
import jline.ConsoleReader;
import jline.Terminal;

public class Main {

	private static Terminal terminal;
	private static ConsoleReader reader;

	public static void main(String[] args) throws Exception {
		try {
			terminal = Terminal.setupTerminal();
			reader = new ConsoleReader();
			terminal.beforeReadLine(reader, "", (char) 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			terminal = null;
		}

		for (String tableName : new String[] { "users", "user_prefs", "user_settings", "students", "recruiters", "students__networks" }) {
			for (int i = 0; i <= 100; i++) {
				setProgress(tableName, i);
			}
			reader.printNewline();
		}

		reader.printString(new ANSIBuffer().green("Operation completed").toString());
		reader.printNewline();
	}

	public static void setProgress(String tableName, int percentage) throws IOException, InterruptedException {
		if (terminal == null) {
			return;
		}

		int termWidth = reader.getTermwidth();
		int w = termWidth;//Math.min(120, termWidth);
		int k = (int) (w * 0.33);
		int s = 1;
		int v = w - k - s;

		int progressValue = (int) Math.floor(percentage / 100. * (v - 7));

		String result = Strings.padEnd(
				Strings.padEnd(tableName, k + s, ' ') +
				Strings.padStart(percentage + "%", 4, ' ') +
				" [" + new ANSIBuffer().green(Strings.repeat("|", progressValue) + Strings.repeat(" ", v - 7 - progressValue)) + "]"
				, termWidth + 9, ' ');

		reader.getCursorBuffer().clearBuffer();
		reader.getCursorBuffer().write(result);
		reader.redrawLine();

		Thread.sleep(1);
	}
}
