package org.apache.storm.starter.DataStrcture;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class OutputToFile {
    private static String filePath = "/home/swhua/log";

    public static void writeToFile(String content) throws IOException {

        File file = null;
        file = new File(filePath);
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
