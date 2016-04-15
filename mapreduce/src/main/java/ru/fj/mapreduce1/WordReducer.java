package ru.fj.mapreduce1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<WordCountPair, Text, LongWritable, Text> {
    // public class WordReducer extends Reducer<LongWritable, Text,
    // LongWritable, Text> {

    private LongWritable wordCount = new LongWritable();
    private Text grouped = new Text();

    // @Override
    // protected void reduce(LongWritable key, Iterable<Text> values,
    // Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws
    // IOException, InterruptedException {

    @Override
    protected void reduce(WordCountPair key, Iterable<Text> values,
            Reducer<WordCountPair, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        StringBuilder result = new StringBuilder();
        for (Text text : values) {
            result.append(text.toString()).append(" ");
        }
        grouped.set(result.toString());
        wordCount.set(key.count);
        context.write(wordCount, grouped);
        // context.write(key, grouped);
    }

}
