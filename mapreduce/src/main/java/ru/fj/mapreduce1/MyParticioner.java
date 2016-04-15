package ru.fj.mapreduce1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyParticioner extends Partitioner<WordCountPair, NullWritable> {
    @Override
    public int getPartition(WordCountPair key, NullWritable value, int numPartitions) {
        // return key.word.hashCode() % numPartitions;
        return Long.hashCode(key.count) % numPartitions;
    }

}
