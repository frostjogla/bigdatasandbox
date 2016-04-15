package ru.fj.accesslogs;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ru.fj.accesslogs.AvgCombiner;
import ru.fj.accesslogs.AvgMapper;
import ru.fj.accesslogs.AvgReducer;

public class AvgByteJobTest {

    private static final LongWritable MAPPER_KEY = new LongWritable(0);

    private final static String MAPPER_INPUT = "ip27 - - [24/Apr/2011:05:36:10 -0400] \"GET /favicon.ico HTTP/1.1\" "
            + "200 318 \"http://host3/\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.6.30 Version/10.62\"";

    private AvgMapper avgMapper;
    private AvgReducer avgReducer;

    private AvgCombiner avgCombiner;

    @Before
    public void setUp() {
        avgMapper = new AvgMapper();
        avgReducer = new AvgReducer();
        avgCombiner = new AvgCombiner();
    }

    @Test
    public void mapperTest() throws IOException {
        new MapDriver<LongWritable, Text, Text, Text>().withMapper(avgMapper)
                .withInput(MAPPER_KEY, new Text(MAPPER_INPUT)).withOutput(new Text("ip27"), new Text("318")).runTest();
    }

    @Test
    public void reducerTest() throws IOException {
        new ReduceDriver<Text, Text, Text, NullWritable>().withReducer(avgReducer)
                .withInput(new Text("ip27"), Arrays.asList(new Text("2,5"), new Text("5,10")))
                .withOutput(new Text("ip27,3.5,15"), NullWritable.get()).runTest();
    }

    @Test
    public void mapReduceText() throws IOException {
        String mapperInput[] = { MAPPER_INPUT,
                "ip27 - - [24/Apr/2011:05:36:10 -0400] \"GET /favicon.ico HTTP/1.1\" "
                        + "200 322 \"http://host3/\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.6.30 Version/10.62\"",
                "ip28 - - [24/Apr/2011:05:36:10 -0400] \"GET /favicon.ico HTTP/1.1\" "
                        + "200 100 \"http://host3/\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.6.30 Version/10.62\"" };

        new MapReduceDriver<LongWritable, Text, Text, Text, Text, NullWritable>().withMapper(avgMapper)
                .withInput(new LongWritable(0), new Text(mapperInput[0]))
                .withInput(new LongWritable(1), new Text(mapperInput[1]))
                .withInput(new LongWritable(3), new Text(mapperInput[2])).withCombiner(avgCombiner)
                .withReducer(avgReducer).withOutput(new Text("ip27,320.0,640"), NullWritable.get())
                .withOutput(new Text("ip28,100.0,100"), NullWritable.get()).runTest();
    }
}
