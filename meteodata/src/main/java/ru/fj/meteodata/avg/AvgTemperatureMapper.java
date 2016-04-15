package ru.fj.meteodata.avg;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ru.fj.meteodata.DataParser;
import ru.fj.meteodata.TemperatureData;

public class AvgTemperatureMapper extends Mapper<LongWritable, Text, DecadeYear, Pair> {

    DataParser dataParser = new DataParser();

    DecadeYear outKey = new DecadeYear();
    Pair outValue = new Pair();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DecadeYear, Pair>.Context context)
            throws IOException, InterruptedException {
        TemperatureData data = dataParser.parse(value.toString());
        outKey.year = data.getYear();
        outValue.sum = data.getTemperature();
        outValue.amount = 1;
        context.write(outKey, outValue);
    }
}
