package ru.fj.meteodata.avg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration avgJobConfig = new Configuration();
        Job avgJob = Job.getInstance(avgJobConfig, "AvgJob");

        avgJob.setJarByClass(AvgJob.class);
        avgJob.setMapperClass(AvgTemperatureMapper.class);
        avgJob.setCombinerClass(AvgTemperatureCombiner.class);
        avgJob.setReducerClass(AvgTemperatureReducer.class);

        avgJob.setMapOutputKeyClass(DecadeYear.class);
        avgJob.setMapOutputValueClass(Pair.class);

        avgJob.setOutputKeyClass(IntWritable.class);
        avgJob.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(avgJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(avgJob, new Path(args[1]));

        avgJob.waitForCompletion(true);
    }
}
