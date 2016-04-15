package ru.fj.meteodata.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ru.fj.meteodata.DataParser;
import ru.fj.meteodata.TemperatureData;

public class TemperatureMapper extends Mapper<LongWritable, Text, Pair, Text> {

    DataParser dataParser = new DataParser();

    Pair outKey = new Pair();
    Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Pair, Text>.Context context)
            throws IOException, InterruptedException {
        TemperatureData data = dataParser.parse(value.toString());

        outKey.first = data.getStationName();
        outKey.second = "1";

        outValue.set(data.getTemperature() + "\t" + data.getYear());

        context.write(outKey, outValue);
    }

}
