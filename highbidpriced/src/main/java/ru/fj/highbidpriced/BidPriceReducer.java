package ru.fj.highbidpriced;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BidPriceReducer extends Reducer<CompositeKey, IntWritable, Text, IntWritable> {

    private Text keyOut = new Text();
    private IntWritable sumOut = new IntWritable();

    @Override
    protected void reduce(CompositeKey key, Iterable<IntWritable> values,
            Reducer<CompositeKey, IntWritable, Text, IntWritable>.Context ctx)
                    throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        keyOut.set(key.city + key.region);
        sumOut.set(sum);
        ctx.write(keyOut, sumOut);
    }
}
