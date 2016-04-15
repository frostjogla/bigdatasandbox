package ru.fj.custominputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxAgeJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration maxAgeConfig = new Configuration();
        Job maxAgeJob = Job.getInstance(maxAgeConfig, "MaxAgeJob");

        maxAgeJob.setInputFormatClass(CsvInputFormat.class);

        CsvInputFormat.setNumLinesPerSplit(maxAgeJob, Integer.valueOf(args[2]));

        String delimiter = ",";
        CsvInputFormat.setDelimiter(maxAgeJob, delimiter);
        CsvInputFormat.setColumnsNumberPerGroup(maxAgeJob, 2);

        maxAgeJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", delimiter);

        maxAgeJob.setJarByClass(MaxAgeJob.class);

        maxAgeJob.setMapperClass(MaxAgeMapper.class);
        maxAgeJob.setCombinerClass(MaxAgeReducer.class);
        maxAgeJob.setReducerClass(MaxAgeReducer.class);

        maxAgeJob.setMapOutputKeyClass(Text.class);
        maxAgeJob.setMapOutputValueClass(IntWritable.class);

        maxAgeJob.setOutputKeyClass(Text.class);
        maxAgeJob.setOutputValueClass(IntWritable.class);

        maxAgeJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(maxAgeJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(maxAgeJob, new Path(args[1]));

        maxAgeJob.waitForCompletion(true);
    }
}
