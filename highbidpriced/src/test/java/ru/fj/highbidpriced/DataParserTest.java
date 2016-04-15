package ru.fj.highbidpriced;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ru.fj.highbidpriced.CompositeKey;
import ru.fj.highbidpriced.DataParser;

public class DataParserTest {

    private DataParser dataParser;

    @Before
    public void setUp() {
        dataParser = new DataParser();
    }

    @Test
    public void getDataTest() throws IOException {
        InputStream resourceStream = this.getClass().getResourceAsStream("head_30.txt");
        List<CompositeKey> lines = new ArrayList<>(5);
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resourceStream, StandardCharsets.UTF_8));) {
            for (String line; (line = reader.readLine()) != null;) {
                CompositeKey data = dataParser.getData(line);
                lines.add(data);
            }
        }
        assertCompositeKey(new CompositeKey("216", "234", "Windows", 277), lines.get(0));
        assertCompositeKey(new CompositeKey("216", "217", "MacOs", 294), lines.get(1));
        assertCompositeKey(new CompositeKey("211", "317", "Android", 298), lines.get(2));
        assertCompositeKey(new CompositeKey("216", "225", "Linux", 294), lines.get(3));
        assertCompositeKey(new CompositeKey("226", "225", "Other", 295), lines.get(4));
    }

    private static void assertCompositeKey(CompositeKey expected, CompositeKey actual) {
        Assert.assertEquals(expected.city, actual.city);
        Assert.assertEquals(expected.region, actual.region);
        Assert.assertEquals(expected.operationSystem, actual.operationSystem);
        Assert.assertEquals(expected.biddingPrice, actual.biddingPrice);
    }
}
