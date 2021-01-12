package org.apache.storm.starter.DataStructure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class OutputToFile {
    private static String logFilePath = "/home/swhua/log";
    private static String matchResultFilePath = "/home/swhua/MatchResult";

    public static void writeToFile(String content) throws IOException {

        File file = null;
        file = new File(logFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("A new file is created.");
        }

        FileWriter fw = new FileWriter(file, false);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();

    }

    public static void saveMatchResult(String content) throws IOException {
        File file = new File(matchResultFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("A new file is created.");
        }

        FileWriter fw = new FileWriter(file, false);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();
    }
}
