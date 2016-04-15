package ru.fj.highbidpriced;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgEventMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text outKey = new Text("1");
    private LongWritable outValue = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        outValue.set(Long.valueOf(split[1]));
        context.write(outKey, outValue);
    }
}
