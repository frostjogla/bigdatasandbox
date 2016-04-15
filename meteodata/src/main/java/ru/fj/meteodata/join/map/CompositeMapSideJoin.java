package ru.fj.meteodata.join.map;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompositeMapSideJoin {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        Path sortedTempPath = new Path(new Path(args[0]).getParent(), "temperature_sorted");
        Path sortedStationPath = new Path(new Path(args[1]).getParent(), "station_sorted");
        Path outPath = new Path(args[2]);
        if (SortTemperatureJob.run(new String[] { args[0], sortedTempPath.toString() })) {
            if (SortStationJob.run(new String[] { args[1], sortedStationPath.toString() })) {
                Configuration jobConfig = new Configuration();

                jobConfig.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
                String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,
                        sortedTempPath, sortedStationPath);
                System.out.println("========" + joinExpression);
                jobConfig.set("mapreduce.join.expr", joinExpression);

                Job joinJob = Job.getInstance(jobConfig, "CompositeMapSideJoin");

                joinJob.setJarByClass(CompositeMapSideJoin.class);
                joinJob.setMapperClass(JoinValuesMapper.class);
                joinJob.setInputFormatClass(CompositeInputFormat.class);
                joinJob.setNumReduceTasks(0);

                FileInputFormat.addInputPath(joinJob, sortedTempPath);
                FileInputFormat.addInputPath(joinJob, sortedStationPath);

                FileOutputFormat.setOutputPath(joinJob, outPath);

                joinJob.waitForCompletion(true);
            }
        }
    }

    static class JoinValuesMapper extends Mapper<Text, TupleWritable, NullWritable, Text> {

        StringBuilder sb = new StringBuilder();
        Text outValue = new Text();

        @Override
        protected void map(Text key, TupleWritable values,
                Mapper<Text, TupleWritable, NullWritable, Text>.Context context)
                        throws IOException, InterruptedException {
            sb.append(key.toString()).append('\t');
            for (Writable writable : values) {
                sb.append(writable.toString()).append('\t');
            }
            sb.setLength(sb.length() - 1);
            outValue.set(sb.toString());
            context.write(NullWritable.get(), outValue);
            sb.setLength(0);
        }
    }
}
