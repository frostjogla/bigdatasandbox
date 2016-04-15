package ru.fj.highbidpriced;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OperationSystemPartitioner extends Partitioner<CompositeKey, IntWritable> {

    @Override
    public int getPartition(CompositeKey key, IntWritable value, int numPartitions) {
        return (key.operationSystem.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

}
