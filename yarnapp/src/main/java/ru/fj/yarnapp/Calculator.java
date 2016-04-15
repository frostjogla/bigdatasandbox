package ru.fj.yarnapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Calculator {

    private static final String HW5_ROOT = "/hw5";

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        System.out.println("[Calculator] start");
        LocalTime startMeasure = LocalTime.now();
        List<Integer> list = new LinkedList<>();
        for (int i = 0; i < 9; i++) {
            fillList(list);
            Collections.sort(list);
            list.clear();
        }
        fillList(list);
        System.out.println("[Calculator] start last iteration sort");
        Collections.sort(list);
        System.out.println("[Calculator] stop last iteration sort");
        try (FileSystem fileSystem = FileSystem.get(URI.create(HW5_ROOT), new Configuration())) {
            Path pathToResult = new Path(HW5_ROOT, "part_" + System.currentTimeMillis() + ".txt");
            System.out.println("[Calculator] start write result to " + pathToResult.getName());
            try (FSDataOutputStream resultOutStream = fileSystem.create(pathToResult);
                    PrintWriter writer = new PrintWriter(resultOutStream)) {
                list.stream().sequential().limit(100).forEach((i) -> writer.println(String.valueOf(i)));
            }
        } catch (IOException e) {
            System.err.println("[Calculator] IOException: " + e.getMessage());
            e.printStackTrace(System.err);
        }
        LocalTime stopMeasure = LocalTime.now();
        Duration between = Duration.between(startMeasure, stopMeasure);
        System.out.println("[Calculator] Stop. Duration: " + between.toMillis() / (double) 1000);
    }

    private static void fillList(List<Integer> list) {
        for (int ii = 0; ii < 10_000_000; ii++) {
            list.add(RANDOM.nextInt());
        }
    }

}
