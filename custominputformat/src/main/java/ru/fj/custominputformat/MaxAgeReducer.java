package ru.fj.custominputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxAgeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable valueOut = new IntWritable();

    @Override
    protected void reduce(Text keyIn, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int maxAge = -1;
        for (IntWritable intWritable : values) {
            maxAge = Math.max(maxAge, intWritable.get());
        }
        valueOut.set(maxAge);
        Text keyOut = keyIn;
        context.write(keyOut, valueOut);
    }
}
