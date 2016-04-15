package ru.fj.highbidpriced;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ByOsJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration byOsConfig = new Configuration();
        Job byOsJob = Job.getInstance(byOsConfig, "ByOsJob");

        byOsJob.setJarByClass(ByOsJob.class);

        byOsJob.setMapperClass(ByOsMapper.class);
        byOsJob.setReducerClass(ByOsReducer.class);

        byOsJob.setMapOutputKeyClass(Text.class);
        byOsJob.setMapOutputValueClass(Text.class);

        byOsJob.setOutputKeyClass(NullWritable.class);
        byOsJob.setOutputValueClass(Text.class);

        byOsJob.setPartitionerClass(OperationSystemPartitioner.class);

        FileInputFormat.addInputPath(byOsJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(byOsJob, new Path(args[1]));

        byOsJob.waitForCompletion(true);
    }
}
