package ru.fj.meteodata.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ru.fj.meteodata.Station;
import ru.fj.meteodata.StationParser;

public class StationMapper extends Mapper<LongWritable, Text, Pair, Text> {

    StationParser stationParser = new StationParser();

    Pair outKey = new Pair();
    Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Pair, Text>.Context context)
            throws IOException, InterruptedException {
        Station station = stationParser.parser(value.toString());

        outKey.first = station.getName();
        outKey.second = "0";

        outValue.set(station.getCity());

        context.write(outKey, outValue);
    }
}
