package ru.fj.highbidpriced;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ByOsReducer extends Reducer<Text, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    protected void reduce(Text osKey, Iterable<Text> groupedByOs, Reducer<Text, Text, NullWritable, Text>.Context ctx)
            throws IOException, InterruptedException {
        for (Text value : groupedByOs) {
            multipleOutputs.write(NullWritable.get(), value, osKey.toString());
        }
    }
}
