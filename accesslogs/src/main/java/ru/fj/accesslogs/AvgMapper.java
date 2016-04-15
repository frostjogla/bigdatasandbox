package ru.fj.accesslogs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgMapper extends Mapper<LongWritable, Text, Text, Text> {

    private DataParser dataParser = new DataParser();

    private Text ip = new Text();
    private Text bytes = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Pair<String, String, String> data = dataParser.getData(value.toString());
        String browser = data.browser;
        if (browser != null) {
            context.getCounter(BrowserGroupCounter.BROWSER.name(), browser).increment(1);
        }
        ip.set(data.ip);
        bytes.set(data.bytes);
        context.write(ip, bytes);
    }
}
