package ru.fj.meteodata.join.reduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ru.fj.meteodata.join.Pair;
import ru.fj.meteodata.join.StationMapper;
import ru.fj.meteodata.join.TemperatureMapper;
import ru.fj.meteodata.join.map.SortTemperatureJob;

public class ReduceJoinJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration reduceJoinJobConfig = new Configuration();
        Job job = Job.getInstance(reduceJoinJobConfig, "ReduceJoinJob");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TemperatureMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StationMapper.class);

        job.setJarByClass(SortTemperatureJob.class);
        job.setReducerClass(ReducJoinReducer.class);

        job.setPartitionerClass(PairPartitioner.class);
        job.setGroupingComparatorClass(Pair.FirstComparator.class);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}
