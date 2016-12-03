package io.quantumdb.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;

public class Cli {

	public static void main(String[] args) throws IOException, SQLException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		String line;
		while ((line = reader.readLine()) != null) {
			if (line.equals("\\q")) {
				return;
			}

			Main.main(line.split(" "));
		}
	}

}
