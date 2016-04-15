package ru.fj.highbidpriced;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ByOsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private OsDataParser osDataParser = new OsDataParser();

    private Text osKeyOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        String operationSystem = osDataParser.getOperationSystem(value.toString());
        osKeyOut.set(operationSystem);
        context.write(osKeyOut, value);
    }
}
