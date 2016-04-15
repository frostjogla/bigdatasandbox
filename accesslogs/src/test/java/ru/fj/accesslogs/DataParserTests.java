package ru.fj.accesslogs;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ru.fj.accesslogs.DataParser;
import ru.fj.accesslogs.Pair;

public class DataParserTests {

    private DataParser dataParser;

    @Before
    public void setUp() {
        dataParser = new DataParser();
    }

    @Test
    public void getDataTest() {
        Pair<String, String, String> data = dataParser
                .getData("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/"
                        + "photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; "
                        + "+http://yandex.com/bots)\"");
        Assert.assertEquals("ip1", data.ip);
        Assert.assertEquals("40028", data.bytes);
        Assert.assertEquals("Mozilla", data.browser);
        data = dataParser.getData("ip21 - - [24/Apr/2011:05:22:59 -0400] \"GET /sunPICS/3_280 HTTP/1.1\" 301 321 \"-\""
                + " \"MLBot (www.metadatalabs.com/mlbot)\"");
        Assert.assertEquals("ip21", data.ip);
        Assert.assertEquals("321", data.bytes);
        Assert.assertEquals("MLBot", data.browser);
    }
}
