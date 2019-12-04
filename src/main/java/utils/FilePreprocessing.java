package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;

public class FilePreprocessing {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Usage: <Full input file path> <Full output file path>");
		}

		String inputFilePath = args[0];
		String outputFilePath = args[1];
		File inputFile = new File(inputFilePath);

		Scanner scanner;
		PrintWriter writer;

		try {

			scanner = new Scanner(inputFile);
			writer = new PrintWriter(outputFilePath);

			while (scanner.hasNext()) {
				String line = scanner.nextLine();
				if (!line.isEmpty()) {
					writer.write(line);
					writer.write("\n");
				}
			}

			scanner.close();
			writer.close();
			System.out.println("Success");

		} catch (FileNotFoundException ex) {
			System.err.println("Input file doesn't exixt.");
		}

	}

}
