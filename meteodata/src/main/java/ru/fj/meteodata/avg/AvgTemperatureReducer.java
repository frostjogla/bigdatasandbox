package ru.fj.meteodata.avg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureReducer extends Reducer<DecadeYear, Pair, IntWritable, DoubleWritable> {

    IntWritable outKey = new IntWritable();
    DoubleWritable outValue = new DoubleWritable();

    Pair pair = new Pair();

    @Override
    protected void reduce(DecadeYear key, Iterable<Pair> values,
            Reducer<DecadeYear, Pair, IntWritable, DoubleWritable>.Context context)
                    throws IOException, InterruptedException {
        for (Pair value : values) {
            pair.amount += value.amount;
            pair.sum += value.sum;
        }
        outKey.set(key.year);
        outValue.set(pair.avg());
        context.write(outKey, outValue);
    }
}
