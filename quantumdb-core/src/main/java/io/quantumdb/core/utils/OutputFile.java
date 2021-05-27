package io.quantumdb.core.utils;

import java.io.File;
import java.io.FileWriter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OutputFile {

	@SneakyThrows
	public static void append(String outputFile, String content) {
		try (FileWriter writer = new FileWriter(new File(outputFile), true)) {
			writer.append(content).append('\n');
		}
	}

}
