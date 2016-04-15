package ru.fj.mapreduce1;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, WordCountPair, Text> {
    // public class WordMapper extends Mapper<LongWritable, Text, LongWritable,
    // Text> {

    private WordCountPair pair = new WordCountPair();
    private Text word = new Text();
//    private LongWritable count = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try (Scanner scanner = new Scanner(value.toString())) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] split = line.split("\\s+");
                pair.word = split[0];
                pair.count = Long.valueOf(split[1]);
                word.set(pair.word);
                context.write(pair, word);
                // word.set(split[0]);
                // count.set(Long.valueOf(split[1]));
                // context.write(count, word);
            }
        }
    }
}
