package ru.fj.custominputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

public class CsvInputFormat extends NLineInputFormat {

    public static final int DEFAULT_COLUMNS_PER_GROUP = 1;
    public static final String DEFAULT_DELIMITER = ",";

    public static final String CSV_DELIMITTER = "csv.delimitter";
    public static final String COLUMNS_NUMBER_PER_GROUP = "csv.columnsnumber.per.group";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new CsvRecordReader(context.getConfiguration().get(CSV_DELIMITTER, DEFAULT_DELIMITER),
                context.getConfiguration().getInt(COLUMNS_NUMBER_PER_GROUP, DEFAULT_COLUMNS_PER_GROUP));
    }

    public static void setDelimiter(Job job, String delimiter) {
        job.getConfiguration().set(CSV_DELIMITTER, delimiter);
    }

    public static void setColumnsNumberPerGroup(Job job, int columnsNumberPerGroup) {
        job.getConfiguration().setInt(COLUMNS_NUMBER_PER_GROUP, columnsNumberPerGroup);
    }
}
