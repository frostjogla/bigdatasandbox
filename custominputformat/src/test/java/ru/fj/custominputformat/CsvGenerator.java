package ru.fj.custominputformat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

public class CsvGenerator {

    private NameGenerator nameGenerator;
    private AgeGenerator ageGenerator;

    @Before
    public void setUp() {
        nameGenerator = new NameGenerator();
        ageGenerator = new AgeGenerator();
    }

    @Test
    public void testGetCsv() throws Exception {
        File file = Paths.get("D:\\", "test.csv").toFile();
        try (FileOutputStream fos = new FileOutputStream(file); PrintWriter csvWriter = new PrintWriter(fos)) {
            for (int i = 0; i < 1000; i++) {
                csvWriter.print(nameGenerator.generateName());
                csvWriter.print(',');
                csvWriter.print(ageGenerator.generateAge());
                csvWriter.print(',');
                csvWriter.print(nameGenerator.generateName());
                csvWriter.print(',');
                csvWriter.print(ageGenerator.generateAge());
                csvWriter.print(',');
                csvWriter.print(nameGenerator.generateName());
                csvWriter.print(',');
                csvWriter.print(ageGenerator.generateAge());
                csvWriter.println();
            }
        }
    }

}
