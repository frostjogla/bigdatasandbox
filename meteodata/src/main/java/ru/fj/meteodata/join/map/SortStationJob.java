package ru.fj.meteodata.join.map;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortStationJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        run(args);
    }

    public static boolean run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path stationPath = new Path(args[0]);
        Path stationSortedPath = new Path(stationPath.getParent(), "station_sorted");
        if (args.length > 1) {
            stationSortedPath = new Path(args[1]);
        }
        Configuration sortStationJobConfig = new Configuration();
        Job temperatureSortJob = Job.getInstance(sortStationJobConfig, "SortStationJob");

        temperatureSortJob.setJarByClass(SortStationJob.class);
        temperatureSortJob.setMapperClass(SortStationJob.StationMapper.class);
        temperatureSortJob.setReducerClass(SortStationJob.StationReducer.class);

        temperatureSortJob.setMapOutputKeyClass(Text.class);
        temperatureSortJob.setMapOutputValueClass(Text.class);

        temperatureSortJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(temperatureSortJob, stationPath);
        FileOutputFormat.setOutputPath(temperatureSortJob, stationSortedPath);

        return temperatureSortJob.waitForCompletion(true);
    }

    static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text keyOut = new Text();
        Text valueOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            keyOut.set(split[0]);
            valueOut.set(split[1]);
            context.write(keyOut, valueOut);
        }
    }

    static class StationReducer extends Reducer<Text, Text, Text, NullWritable> {

        Text outKey = new Text();

        @Override
        protected void reduce(Text inKey, Iterable<Text> values,
                Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                outKey.set(inKey.toString() + "\t" + value.toString());
                context.write(outKey, NullWritable.get());
            }
        }
    }

}
