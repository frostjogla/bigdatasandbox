package ru.fj.sorthdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.util.ReflectionUtils;

class BigFileSorter {

    private static final int DEFAULT_FACTOR = 4;
    private final static String DIR_FOR_SORTED = "/sorted";
    private static final String DIR_FOR_MERGED = "/merged";

    private final Path inputDirectory;
    private final Path sortedDirectory;
    private final Path outDirectory;

    private final FileSystem fileSystem;
    private final CompressionCodec codec;

    private ValueGetter<String, String> valueGetter;
    private SortOrder order;
    private int factor = DEFAULT_FACTOR;
    private List<String> partLines;
    private Path resultPath;

    public BigFileSorter(String in, Configuration conf) throws IOException, ClassNotFoundException {

        partLines = new LinkedList<>();

        inputDirectory = new Path(in);
        sortedDirectory = new Path(in + DIR_FOR_SORTED);
        outDirectory = new Path(in + DIR_FOR_MERGED);
        resultPath = new Path(outDirectory, "result.txt");

        System.out.println("     imputDir: " + inputDirectory);
        System.out.println("sortedPartDir: " + sortedDirectory);
        System.out.println("    outputDir: " + outDirectory);

        fileSystem = FileSystem.get(inputDirectory.toUri(), conf);

        fileSystem.mkdirs(inputDirectory);
        fileSystem.mkdirs(sortedDirectory);
        fileSystem.mkdirs(outDirectory);

        String codecClassname = BZip2Codec.class.getCanonicalName();
        Class<?> codecClass = Class.forName(codecClassname);

        codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

    }

    public Path doSort() throws FileNotFoundException, IOException {
        splitAndSortParts();
        mergeFile();
        return resultPath;
    }

    private void splitAndSortParts() throws FileNotFoundException, IOException {
        RemoteIterator<LocatedFileStatus> unsortedFiles = fileSystem.listFiles(inputDirectory, false);
        int i = 0;
        while (unsortedFiles.hasNext()) {
            LocatedFileStatus next = unsortedFiles.next();
            System.out.println("Start Split And Sort file: " + next.getPath().getName());
            if (next.isDirectory()) {
                continue;
            }
            try (CompressionInputStream cis = codec.createInputStream(fileSystem.open(next.getPath()));
                    InputStreamReader isr = new InputStreamReader(cis);
                    BufferedReader reader = new BufferedReader(isr)) {
                for (String readedLine; (readedLine = reader.readLine()) != null;) {
                    partLines.add(readedLine);
                    if (endReading(partLines)) {
                        createSortedFile(partLines, i);
                        i++;
                    }
                }
            }
        }
        createSortedFile(partLines, i);
    }

    private int getIndexOfValue(SortOrder order, List<String> values) {
        int result = -1;
        String prevValue = null;
        int i = 0;
        for (; i < values.size(); i++) {
            if (values.get(i) == null) {
                continue;
            } else {
                result = i;
                prevValue = values.get(i);
                break;
            }
        }
        if (result < 0) {
            return result;
        }
        for (int j = i; j < values.size(); j++) {
            if (values.get(j) != null) {
                if (order == SortOrder.DESC) {
                    if (values.get(j).compareTo(prevValue) > 0) {
                        result = j;
                        prevValue = values.get(j);
                    }
                } else {
                    if (values.get(j).compareTo(prevValue) < 0) {
                        result = j;
                        prevValue = values.get(j);
                    }
                }
            }
        }
        return result;
    }

    private void createSortedFile(List<String> partLines, int i) throws IOException, FileNotFoundException {
        Path sortedFile = new Path(sortedDirectory, "part_" + i + ".txt");
        try (FSDataOutputStream fos = fileSystem.create(sortedFile, true); PrintWriter writer = new PrintWriter(fos)) {
            Collections.sort(partLines, (line1, line2) -> this.order.value
                    * valueGetter.getValue(line1).compareTo(valueGetter.getValue(line2)));
            for (String line : partLines) {
                writer.println(line);
            }
            partLines.clear();
        }
        System.out.println("Sorted File was created: " + sortedFile.getName());
    }

    private long arraySize = 0;

    private boolean endReading(List<String> unsortedPart) {
        Runtime runtime = Runtime.getRuntime();
        arraySize += 36 + unsortedPart.get(unsortedPart.size() - 1).length() * 2;
        long currentFactor = (runtime.totalMemory() / arraySize);
        boolean result = currentFactor < factor;
        if (result) {
            arraySize = 0;
        }
        return result;
        // return unsortedPart.size() > getFactor();
    }

    private void mergeFile() throws FileNotFoundException, IOException {
        RemoteIterator<LocatedFileStatus> sortedFiles = fileSystem.listFiles(sortedDirectory, false);

        List<BufferedReader> readers = new ArrayList<>();
        List<String> readedValues = new ArrayList<>();

        try {
            while (sortedFiles.hasNext()) {
                readers.add(new BufferedReader(
                        new InputStreamReader(fileSystem.open(sortedFiles.next().getPath()), StandardCharsets.UTF_8)));
            }
            for (int j = 0; j < readers.size(); j++) {
                readedValues.add(j, this.valueGetter.getValue(readers.get(j).readLine()));
            }
            // try (CompressionOutputStream bz2OutStream =
            // codec.createOutputStream(fileSystem.create(resultPath));
            // PrintWriter resultWriter = new PrintWriter(bz2OutStream)) {
            try (FSDataOutputStream bz2OutStream = fileSystem.create(resultPath);
                    PrintWriter resultWriter = new PrintWriter(bz2OutStream)) {
                int indexOfValue;
                while ((indexOfValue = getIndexOfValue(this.order, readedValues)) > -1) {
                    resultWriter.println(readedValues.get(indexOfValue));
                    String readLine = readers.get(indexOfValue).readLine();
                    if (readLine != null) {
                        readedValues.set(indexOfValue, this.valueGetter.getValue(readLine));
                    } else {
                        readedValues.set(indexOfValue, null);
                    }
                }
            }
        } finally {
            for (BufferedReader bufferedReader : readers) {
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }
                }
            }
        }
    }

    ValueGetter<String, String> getValueGetter() {
        return valueGetter;
    }

    void setValueGetter(ValueGetter<String, String> valueGetter) {
        this.valueGetter = valueGetter;
    }

    SortOrder getOrder(SortOrder order) {
        return this.order;
    }

    void setOrder(SortOrder order) {
        this.order = order;
    }

    int getFactor() {
        return factor;
    }

    void setFactor(int factor) {
        this.factor = factor;
    }

}
