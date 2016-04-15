package ru.fj.accesslogs1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReducer extends Reducer<Text, Text, Text, NullWritable> {

    private Text csvLine = new Text();
    private NullWritable nullValue = NullWritable.get();

    @Override
    protected void reduce(Text ip, Iterable<Text> bytes, Reducer<Text, Text, Text, NullWritable>.Context ctx)
            throws IOException, InterruptedException {
        long sum = 0L;
        double avgSum = 0;
        long size = 0L;
        for (Text textByte : bytes) {
            ++size;
            String[] split = textByte.toString().split(",");
            try {
                avgSum += Double.parseDouble(split[0]);
            } catch (NumberFormatException e) {
                avgSum += 0.d;
            }
            try {
                sum += Long.parseLong(split[1]);
            } catch (NumberFormatException e) {
                sum += 0;
            }
        }
        StringBuilder sb = new StringBuilder(ip.toString()).append(',');
        if (size == 0L) {
            sb.append("-");
        } else {
            sb.append(avgSum / (double) size);
        }
        sb.append(',').append(sum);
        csvLine.set(sb.toString());
        ctx.write(csvLine, nullValue);
    }
}
