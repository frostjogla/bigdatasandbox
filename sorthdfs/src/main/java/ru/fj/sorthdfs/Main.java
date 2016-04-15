package ru.fj.sorthdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

    private static final String HW2_ROOT = "/hw2";

    private static final String INPUT_DIR = HW2_ROOT + "/ipinyou.contest.dataset";

    /**
     * export HADOOP_CLIENT_OPTS=
     * "-Xms6G -Xmx6G -XX:+UseG1GC -XX:+UseStringDeduplication" hadoop jar
     * /home/hdfs/ru.fj.sorthdfs-0.0.1-SNAPSHOT.jar
     * ru.fj.sorthdfs.SortHdfs
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        long startTime = System.currentTimeMillis();
        System.out.println("Start Time: " + startTime);
        try (FileSystem fileSystem = FileSystem.get(URI.create(HW2_ROOT), config)) {
            int partSize = Integer.valueOf(args[0]);
            BigFileSorter sorter = new BigFileSorter(INPUT_DIR, config);
            sorter.setValueGetter(new IpinyouIDValueGetter());
            sorter.setOrder(SortOrder.DESC);
            sorter.setFactor(partSize);

            long sortingStart = System.currentTimeMillis();
            System.out.println("Start Sorting: " + sortingStart);
            Path pathToSortedFile = sorter.doSort();
            long sortingStop = System.currentTimeMillis();
            System.out.println("Total Sorting Time: " + (sortingStop - sortingStart) / 1000 + " s");

            TokenCounter counter = new TokenCounter();
            counter.setOrder(SortOrder.DESC);
            long countingStart = System.currentTimeMillis();
            System.out.println("Start Counting: " + countingStart);
            counter.doCount(fileSystem, pathToSortedFile);
            long countingStop = System.currentTimeMillis();
            System.out.println("Total Counting Time: " + (countingStop - countingStart) / 1000 + " s");
        }
        long finishTime = System.currentTimeMillis();
        System.out.println("Finish Time: " + finishTime);
        System.out.println("Total Time: " + ((finishTime - startTime) / 1000) + " s");
    }

}
