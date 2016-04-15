package ru.fj.highbidpriced;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgEventReducer extends Reducer<Text, LongWritable, DoubleWritable, NullWritable> {

    private DoubleWritable outValue = new DoubleWritable();

    @Override
    protected void reduce(Text inKey, Iterable<LongWritable> inValues,
            Reducer<Text, LongWritable, DoubleWritable, NullWritable>.Context ctx)
                    throws IOException, InterruptedException {
        int sum = 0;
        int size = 0;
        for (LongWritable longWritable : inValues) {
            sum += longWritable.get();
            ++size;
        }
        outValue.set(sum / (double) size);
        ctx.write(outValue, NullWritable.get());
    }
}
