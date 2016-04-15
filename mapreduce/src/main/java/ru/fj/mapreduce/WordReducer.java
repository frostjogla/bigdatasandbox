package ru.fj.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    LongWritable sumResult = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, LongWritable>
            .Context context) throws IOException, InterruptedException {
        long lsum = 0;
        for (IntWritable value : values) {
            lsum += value.get();
        }
        sumResult.set(lsum);
        context.write(key, sumResult);
    }

}
