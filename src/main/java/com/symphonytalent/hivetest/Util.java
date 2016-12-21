package com.symphonytalent.hivetest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public final class Util {
    public static List<String> readFile(String path) {
        try {
            return Files.readAllLines(Paths.get(path));
        } catch (IOException ex) {
            System.err.println("Caught IOException: " + ex.getMessage());
            return null;
        }
    }

    public static String joinStringCollection(List<String> coll) {
        return String.join("\n", coll);
    }

    public static String transformQueryFileToStatementString(String filepath) {
        return joinStringCollection(readFile(filepath)).replace(";", "");
    }
}
