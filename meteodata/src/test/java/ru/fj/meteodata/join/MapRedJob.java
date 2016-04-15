package ru.fj.meteodata.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import ru.fj.meteodata.join.Pair;
import ru.fj.meteodata.join.StationMapper;
import ru.fj.meteodata.join.TemperatureMapper;
import ru.fj.meteodata.join.reduce.ReducJoinReducer;

public class MapRedJob {
    private StationMapper stationMapper;
    private TemperatureMapper temperatureMapper;
    private ReducJoinReducer reducJoinReducer;

    @Before
    public void setUp() {
        stationMapper = new StationMapper();
        temperatureMapper = new TemperatureMapper();
        reducJoinReducer = new ReducJoinReducer();
    }

    @Test
    public void stationMapperTest() throws Exception {
        LongWritable key1 = new LongWritable(0);
        new MapDriver<LongWritable, Text, Pair, Text>().withMapper(stationMapper)
                .withInput(key1, new Text("station1\tcity1")).withOutput(new Pair("station1", "0"), new Text("city1"))
                .runTest();
    }

    @Test
    public void temperatureMapperTest() throws Exception {
        LongWritable key1 = new LongWritable(0);
        new MapDriver<LongWritable, Text, Pair, Text>().withMapper(temperatureMapper)
                .withInput(key1, new Text("station1\t22\t1994"))
                .withOutput(new Pair("station1", "1"), new Text("22\t1994")).runTest();
    }

}
