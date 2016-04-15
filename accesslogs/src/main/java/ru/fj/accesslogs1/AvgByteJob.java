package ru.fj.accesslogs1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ru.fj.accesslogs.BrowserGroupCounter;

/**
 * hadoop jar /home/hdfs/hw3/ru.fj.accesslogs-0.0.1-SNAPSHOT.jar
 * ru.fj.accesslogs.AvgByteJob /tmp/hw3/in/ /tmp/hw3/out/
 */
public class AvgByteJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();

        config.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(config, "avgbytejob");
        job.setJarByClass(AvgByteJob.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(AvgMapper.class);
        job.setCombinerClass(AvgCombiner.class);
        job.setReducerClass(AvgReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        CounterGroup group = job.getCounters().getGroup(BrowserGroupCounter.BROWSER.name());
        for (Counter counter : group) {
            System.out.println("==" + counter.getName() + " = " + counter.getValue());
        }
    }
}
