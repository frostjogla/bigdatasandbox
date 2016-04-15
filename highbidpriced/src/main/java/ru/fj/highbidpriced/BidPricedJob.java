package ru.fj.highbidpriced;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BidPricedJob {

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

        Path byOsJobOut = new Path(args[0], "byOsJobOut");
        FileOutputFormat.setOutputPath(byOsJob, byOsJobOut);

        if (byOsJob.waitForCompletion(true)) {
            Configuration bidPricedConfig = new Configuration();
            Job jobBidPriced = Job.getInstance(bidPricedConfig, "bidpricedjob");

            jobBidPriced.setJarByClass(BidPricedJob.class);

            jobBidPriced.setMapperClass(BidPriceMapper.class);
            jobBidPriced.setReducerClass(BidPriceReducer.class);

            jobBidPriced.setMapOutputKeyClass(CompositeKey.class);
            jobBidPriced.setMapOutputValueClass(IntWritable.class);

            jobBidPriced.setOutputKeyClass(LongWritable.class);
            jobBidPriced.setOutputValueClass(NullWritable.class);

            jobBidPriced.setInputFormatClass(TextInputFormat.class);
            jobBidPriced.setOutputFormatClass(TextOutputFormat.class);
            jobBidPriced.setNumReduceTasks(Integer.valueOf(args[2]));

            Path bidPricedOut = new Path(args[0], "bidPricedOut");

            FileInputFormat.addInputPath(jobBidPriced, byOsJobOut);
            FileOutputFormat.setOutputPath(jobBidPriced, bidPricedOut);

            if (jobBidPriced.waitForCompletion(true)) {

                Configuration avgEventConfig = new Configuration();
                Job avgEventJob = Job.getInstance(avgEventConfig, "avgeventjob");
                avgEventJob.setJarByClass(AvgEventJob.class);

                avgEventJob.setMapperClass(AvgEventMapper.class);
                avgEventJob.setReducerClass(AvgEventReducer.class);

                avgEventJob.setMapOutputKeyClass(Text.class);
                avgEventJob.setMapOutputValueClass(LongWritable.class);

                avgEventJob.setOutputKeyClass(DoubleWritable.class);
                avgEventJob.setOutputValueClass(NullWritable.class);

                avgEventJob.setInputFormatClass(TextInputFormat.class);
                avgEventJob.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(avgEventJob, bidPricedOut);
                FileOutputFormat.setOutputPath(avgEventJob, new Path(args[1]));

                System.exit(avgEventJob.waitForCompletion(true) ? 0 : 1);
            } else {
                System.exit(1);
            }
        } else {
            System.exit(1);
        }
    }

}
