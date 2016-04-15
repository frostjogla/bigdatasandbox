package ru.fj.accesslogs1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgCombiner extends Reducer<Text, Text, Text, Text> {

    private Text csvLine = new Text();

    @Override
    protected void reduce(Text ip, Iterable<Text> bytes, Reducer<Text, Text, Text, Text>.Context ctx)
            throws IOException, InterruptedException {
        long sum = 0L;
        long size = 0L;
        for (Text textByte : bytes) {
            ++size;
            String strByte = textByte.toString();
            try {
                sum += Long.parseLong(strByte);
            } catch (NumberFormatException e) {
                sum += 0;
            }
        }
        csvLine.set(new StringBuilder().append(sum / (double) size).append(',').append(sum).toString());
        ctx.write(ip, csvLine);
    }
}
