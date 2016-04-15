package ru.fj.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Pattern WORD_PATTERN = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = WORD_PATTERN.matcher(value.toString());
        while (matcher.find()) {
            word.set(matcher.group());
            context.write(word, ONE);
        }
    }
}
