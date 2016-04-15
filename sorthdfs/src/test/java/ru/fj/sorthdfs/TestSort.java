package ru.fj.sorthdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Files;

import ru.fj.sorthdfs.IpinyouIDValueGetter;
import ru.fj.sorthdfs.Pair;
import ru.fj.sorthdfs.SortOrder;

@Ignore
public class TestSort {

    private static final String D_COURSES_HW_2_TEST = "D:\\courses\\HW-2\\test";

    static final Path PATH_TO_UNSORTED = Paths.get(D_COURSES_HW_2_TEST);
    static final Path PATH_TO_SORTED = Paths.get(D_COURSES_HW_2_TEST, "sorted");
    static final Path PATH_TO_MERGED = Paths.get(D_COURSES_HW_2_TEST, "merged");

    private IpinyouIDValueGetter valueGetter;
    private SortOrder order;

    @Before
    public void setUp() {
        this.valueGetter = new IpinyouIDValueGetter();
        this.order = SortOrder.DESC;
    }

    @BeforeClass
    public static void BeforeClass() throws IOException {
        File un = PATH_TO_UNSORTED.toFile();
        if (!un.exists()) {
            throw new IOException();
        }
        File so = PATH_TO_SORTED.toFile();
        if (!so.exists()) {
            if (!so.mkdirs()) {
                throw new IOException();
            }
        }
        File me = PATH_TO_MERGED.toFile();
        if (!me.exists()) {
            if (!me.mkdirs()) {
                throw new IOException();
            }
        }
    }

    @Test
    public void mapFilesTest() throws IOException {
        File rootPath = PATH_TO_UNSORTED.toFile();
        File[] unsortedFiles = rootPath.listFiles();
        List<String> partLines = new ArrayList<>();
        int i = 0;
        for (File unsortedFile : unsortedFiles) {
            if (unsortedFile.isDirectory()) {
                continue;
            }
            try (FileInputStream fis = new FileInputStream(unsortedFile);
                    InputStreamReader isr = new InputStreamReader(fis);
                    BufferedReader reader = new BufferedReader(isr);) {
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

    @Test
    public void mergeFiles() throws FileNotFoundException, IOException {
        Iterator<File> sortedFiles = Arrays.asList(PATH_TO_SORTED.toFile().listFiles()).iterator();

        List<BufferedReader> readers = new ArrayList<>();
        List<String> readedValues = new ArrayList<>();

        try {
            while (sortedFiles.hasNext()) {
                readers.add(new BufferedReader(
                        new InputStreamReader(new FileInputStream(sortedFiles.next()), StandardCharsets.UTF_8)));
            }
            File mergedFile = Paths.get(PATH_TO_MERGED.toString(), "merged.txt").toFile();
            if (!mergedFile.createNewFile()) {
                throw new IOException("Can't create file " + mergedFile.getAbsolutePath());
            }
            for (int j = 0; j < readers.size(); j++) {
                readedValues.add(j, this.valueGetter.getValue(readers.get(j).readLine()));
            }
            try (FileOutputStream fos = new FileOutputStream(mergedFile);
                    PrintWriter resultWriter = new PrintWriter(fos)) {
                int indexOfValue;
                while ((indexOfValue = getIndexOfValue(order, readedValues)) > -1) {
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
                    bufferedReader.close();
                }
            }
        }

    }

    private static boolean isAllFilesReaded(List<String> readedValues) {
        boolean allRecordsIsNull = true;
        for (int i = 0; i < readedValues.size(); i++) {
            if (readedValues.get(i) != null) {
                return false;
            }
        }
        return allRecordsIsNull;
    }

    @Test
    public void isAllFilesReadedTest() {
        Assert.assertFalse(isAllFilesReaded(Arrays.asList(null, null, "1", null, "2")));
        Assert.assertTrue(isAllFilesReaded(Arrays.asList(null, null, null, null, null, null)));
    }

    @Test
    public void getIndexOfValueTest() {
        List<String> array = Arrays.asList("d", "a", "r", null, "t", "null", "z", "x");
        Assert.assertEquals(6, getIndexOfValue(SortOrder.ASC, array));
        Assert.assertEquals(1, getIndexOfValue(SortOrder.DESC, array));
        Assert.assertEquals(-1, getIndexOfValue(SortOrder.ASC, Arrays.asList(null, null, null, null, null)));
        Assert.assertEquals(-1, getIndexOfValue(SortOrder.DESC, Arrays.asList(null, null, null, null, null)));
    }

    public static int getIndexOfValue(SortOrder order, List<String> values) {
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

    @Test
    public void compareFiles() throws IOException {
        List<String> unsortedLines = readLinesAndSort(PATH_TO_UNSORTED);
        List<String> sortedLines = readLinesAndSort(PATH_TO_SORTED);
        Assert.assertEquals(unsortedLines, sortedLines);
    }

    @Test
    public void finalComparision() throws IOException {
        List<String> unsortedLines = readLinesAndSort(PATH_TO_UNSORTED);
        List<String> sortedLines = readLinesAndSort(PATH_TO_SORTED);
        // List<String> mergedLines = readLinesAndSort(PATH_TO_MERGED);
        Assert.assertEquals(unsortedLines, sortedLines);
        // Assert.assertEquals(sortedLines, mergedLines);
    }

    private List<String> readLinesAndSort(Path path) throws IOException {
        File rootPath = path.toFile();
        File[] files = rootPath.listFiles();
        List<String> lines = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                continue;
            }
            lines.addAll(Files.readLines(file, StandardCharsets.UTF_8));
        }
        Collections.sort(lines,
                (s1, s2) -> this.order.value * this.valueGetter.getValue(s1).compareTo(this.valueGetter.getValue(s2)));
        return lines;
    }

    private void createSortedFile(List<String> partLines, int i) throws IOException, FileNotFoundException {
        File sortedFile = Paths.get(PATH_TO_SORTED.toFile().getAbsolutePath(), i + "sorted.txt").toFile();
        if (!sortedFile.exists()) {
            if (!sortedFile.createNewFile()) {
                throw new IOException("Can't create file " + sortedFile.getAbsolutePath());
            }
        }
        try (FileOutputStream fos = new FileOutputStream(sortedFile); PrintWriter writer = new PrintWriter(fos)) {
            Collections.sort(partLines, (s1, s2) -> this.order.value
                    * this.valueGetter.getValue(s1).compareTo(this.valueGetter.getValue(s2)));
            for (String line : partLines) {
                writer.println(line);
            }
            partLines.clear();
        }
    }

    @Test
    public void getId() throws IOException {
        String line = Files.readLines(new File("d:\\courses\\HW-2\\outBz2.txt\\outBz2.txt"), StandardCharsets.UTF_8)
                .get(0);
        // int from = line.indexOf('\t', line.indexOf('\t') + 1);
        // int to = line.indexOf('\t', from + 1);
        // System.out.println(line.substring(from + 1, to));
        IpinyouIDValueGetter ipinyouIDValueGetter = new IpinyouIDValueGetter();
        System.out.println(ipinyouIDValueGetter.getValue(line));
    }

    @Test
    public void memTest() {
        List<String> list = new LinkedList<>();
        Runtime runtime = Runtime.getRuntime();
        long arraySize = 0;
        long factor = Long.MAX_VALUE;
        while (true) {
            String str = "" + System.currentTimeMillis();
            list.add(str);
            runtime = Runtime.getRuntime();
            arraySize += 36 + str.length() * 2;
            long currentFactor = runtime.totalMemory() / arraySize;
            if (currentFactor < 5) {
                // arraySize = 0;
                break;
            }
        }
        System.out.println(" usedMemory = " + (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024) + " Mb");
        System.out.println("totalMemory = " + runtime.totalMemory() / (1024 * 1024) + " Mb");
        System.out.println("     factor = " + factor);
        System.out.println("list.size() = " + list.size());
        System.out.println("       list = " + arraySize / (1024 * 1024) + " Mb");
    }

    @Test
    public void countAndSort() throws FileNotFoundException, IOException {
        File sortedFile = PATH_TO_MERGED.toFile().listFiles()[0];
        Path fileFor = Paths.get(PATH_TO_MERGED.toFile().getAbsolutePath(), "final_result.txt");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(sortedFile)));
                PrintWriter writer = new PrintWriter(new FileOutputStream(fileFor.toFile()))) {
            SortOrder order = SortOrder.DESC;
            List<Pair> list = new LinkedList<>();
            Pair prevPair = null;
            for (String readedLine; (readedLine = reader.readLine()) != null;) {
                if (prevPair != null && readedLine.equals(prevPair.key)) {
                    ++prevPair.value;
                } else {
                    if (prevPair != null) {
                        list.add(prevPair);
                    }
                    prevPair = new Pair(readedLine, 1);
                }
            }
            list.add(prevPair);
            Collections.sort(list, (o1, o2) -> order.value * Integer.compare(o1.value, o2.value));
            for (Pair pair : list) {
                writer.println(pair.toString());
            }
        }
    }

    private boolean endReading(List<String> unsortedPart) {
        int length = 0;
        switch ((int) (System.nanoTime() % 3L)) {
        case 0:
            length = 10;
            break;
        case 1:
            length = 11;
            break;
        default:
            length = 12;
            break;
        }
        return unsortedPart.size() > length;
    }
}
