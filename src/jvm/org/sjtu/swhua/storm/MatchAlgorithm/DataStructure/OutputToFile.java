package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class OutputToFile {
    private static String baseFilePath;
    private static String logFilePath;
    private static String matchResultFilePath;
    private static String speedFilePath;
    private static String otherInfoFilePath;
    private static String errorLogFilePath;

    public OutputToFile() {
        baseFilePath = TypeConstant.baseLogFilePath;
        logFilePath = baseFilePath + "log";
        matchResultFilePath = baseFilePath + "MatchResult";
        speedFilePath = baseFilePath + "speed";
        otherInfoFilePath = baseFilePath + "otherInfo";
        errorLogFilePath = baseFilePath + "errorLog";
    }

    public static void writeToLogFile(String content) throws IOException {
        File file = new File(logFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("Log file is created.\n");
        }

        FileWriter fw = new FileWriter(file, true); // true means add to the tail of the file, no coverage
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();

    }

    public static void saveMatchResult(String content) throws IOException {
        File file = new File(matchResultFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("MatchResult file is created.\n");
        }

        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();
    }

    public static void recordSpeed(String content) throws IOException {
        File file = new File(speedFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("Speed file is created.\n");
        }

        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();
    }

    public static void otherInfo(String content) throws IOException {
        File file = new File(otherInfoFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("\"OtherInfo\" file is created.\n");
        }

        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();
    }

    public static void errorLog(String content) throws IOException {
        File file = new File(errorLogFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("\"ErrorLog\" file is created.\n");
        }

        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();
    }
}
