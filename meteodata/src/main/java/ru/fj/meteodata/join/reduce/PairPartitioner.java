package ru.fj.meteodata.join.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import ru.fj.meteodata.join.Pair;

public class PairPartitioner extends Partitioner<Pair, Text> {
    @Override
    public int getPartition(Pair key, Text value, int numPartitions) {
        return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
