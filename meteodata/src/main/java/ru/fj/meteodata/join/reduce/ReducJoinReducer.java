package ru.fj.meteodata.join.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ru.fj.meteodata.join.Pair;

public class ReducJoinReducer extends Reducer<Pair, Text, Text, NullWritable> {

    @Override
    protected void reduce(Pair inKey, Iterable<Text> values, Reducer<Pair, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Iterator<Text> valueIterator = values.iterator();
        String city = valueIterator.next().toString();
        while (valueIterator.hasNext()) {
            Text temperature = valueIterator.next();
            Text outKey = new Text();
            String string = inKey.getFirst() + "\t" + temperature.toString() + "\t" + city;
            outKey.set(string);
            context.write(outKey, NullWritable.get());
        }
    }
}
