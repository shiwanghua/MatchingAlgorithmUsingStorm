package org.apache.storm.starter.DataStructure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class OutputToFile {
    private static final String logFilePath = "/home/swhua/log";
    private static final String matchResultFilePath = "/home/swhua/MatchResult";
    private static final String speedFilePath = "/home/swhua/speed";
    public OutputToFile(){

    }

    public static void writeToLogFile(String content) throws IOException {

        File file = null;
        file = new File(logFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("Log file is created.");
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
            System.out.println("MatchResult file is created.");
        }

        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();
    }

    public static void recordSpeed(String content) throws IOException{
        File file = new File(speedFilePath);
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("Speed file is created.");
        }

        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();
    }
}
