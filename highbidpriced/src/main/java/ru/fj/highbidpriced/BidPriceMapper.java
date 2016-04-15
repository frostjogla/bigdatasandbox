package ru.fj.highbidpriced;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BidPriceMapper extends Mapper<LongWritable, Text, CompositeKey, IntWritable> {

    private DataParser dataParser = new DataParser();

    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text line,
            Mapper<LongWritable, Text, CompositeKey, IntWritable>.Context context)
                    throws IOException, InterruptedException {
        CompositeKey outKey = dataParser.getData(line.toString());
        if (outKey.biddingPrice > 200) {
            context.write(outKey, ONE);
        }
    }
}