package ru.fj.meteodata.join.map.cache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ru.fj.meteodata.DataParser;
import ru.fj.meteodata.TemperatureData;

public class MapSideJoinCacheJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration jobConfig = new Configuration();
        Job job = Job.getInstance(jobConfig, "MapSideJoinCacheJob");

        job.setJarByClass(MapSideJoinCacheJob.class);
        job.setMapperClass(MapSideJoinCacheJob.MapSideJoinCacheMapper.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.addCacheFile(new Path(args[1]).toUri());
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

    static class MapSideJoinCacheMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        DataParser dataParser = new DataParser();
        Text outValue = new Text();

        /* [station name][tab][city] */
        private Map<String, String> stations = new HashMap<String, String>();

        private void readStations(URI uri, Context context) throws IOException {
            System.out.println("========" + uri.toString());
            FileSystem fs = FileSystem.get(uri, context.getConfiguration());
            try (FSDataInputStream open = fs.open(new Path(uri));
                    InputStreamReader in = new InputStreamReader(open, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(in)) {
                for (String line; (line = reader.readLine()) != null;) {
                    String[] split = line.split("\t");
                    System.out.println("=====" + split[0] + " " + split[1]);
                    stations.put(split[0], split[1]);
                }
            }
        }

        @Override
        protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            readStations(cacheFiles[0], context);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {
            TemperatureData temperatureData = dataParser.parse(value.toString());
            StringBuilder sb = new StringBuilder();
            sb.append(temperatureData.getStationName()).append('\t').append(temperatureData.getTemperature())
                    .append('\t').append(temperatureData.getYear()).append('\t')
                    .append(stations.get(temperatureData.getStationName()));
            outValue.set(sb.toString());
            context.write(NullWritable.get(), outValue);
        }
    }
}
