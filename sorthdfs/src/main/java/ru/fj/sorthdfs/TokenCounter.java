package ru.fj.sorthdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class TokenCounter {

    private SortOrder order;

    void doCount(FileSystem fileSystem, Path pathToSortedFile) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fileSystem.open(pathToSortedFile), StandardCharsets.UTF_8));
                PrintWriter writer = new PrintWriter(
                        fileSystem.create(new Path(pathToSortedFile.getParent(), "final_result.txt")))) {
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

    public void setOrder(SortOrder order) {
        this.order = order;
    }
}
