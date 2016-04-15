package ru.fj.custominputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxAgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable();

    private String delimiter;

    private DataParser dataParser;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        delimiter = context.getConfiguration().get(CsvInputFormat.CSV_DELIMITTER, CsvInputFormat.DEFAULT_DELIMITER);
        dataParser = new DataParser(delimiter);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        dataParser.parse(value.toString());
        outKey.set(dataParser.getName());
        outValue.set(dataParser.getAge());
        context.write(outKey, outValue);
    }
}
