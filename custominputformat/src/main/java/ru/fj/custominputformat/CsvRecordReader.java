package ru.fj.custominputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class CsvRecordReader extends LineRecordReader {

    private int columnsNumberPerGroup;
    private String delimiter;
    /** package-private for test purpose */
    String readedLine;
    private int startIndex;
    private int endIndex;
    private Text value;

    public CsvRecordReader(String delimiter, int columnsNumberPerGroup) {
        this.delimiter = delimiter;
        this.columnsNumberPerGroup = columnsNumberPerGroup;
        this.startIndex = -1;
        this.endIndex = -1;
        this.value = new Text();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        boolean nextKeyValue = false;
        if (readedLine != null) {
            startIndex = endIndex + 1;
            nextKeyValue = readedLine.length() > startIndex + 1;
            if (nextKeyValue) {
                endIndex();
            } else {
                readedLine = null;
            }
        }
        if (readedLine == null) {
            startIndex = 0;
            endIndex = -1;
            nextKeyValue = nextKeyValueWrapper();
            Text superCurrentValue = getCurrentValueWrapper();
            if (superCurrentValue != null) {
                readedLine = superCurrentValue.toString();
            }
            if (nextKeyValue) {
                endIndex();
            }
        }
        return nextKeyValue;
    }

    Text getCurrentValueWrapper() {
        return super.getCurrentValue();
    }

    boolean nextKeyValueWrapper() throws IOException {
        return super.nextKeyValue();
    }

    private void endIndex() {
        for (int i = 0; i < columnsNumberPerGroup; i++) {
            int indexOf = readedLine.indexOf(delimiter, endIndex + 1);
            if (indexOf > -1) {
                endIndex = indexOf;
            } else {
                endIndex = readedLine.length();
                break;
            }
        }
    }

    @Override
    public Text getCurrentValue() {
        Text currentValue = value;
        if (readedLine != null) {
            currentValue.set(readedLine.substring(startIndex, endIndex));
        } else {
            value = null;
        }
        return currentValue;
    }

    @Override
    public LongWritable getCurrentKey() {
        LongWritable currentKey = super.getCurrentKey();
        currentKey.set(currentKey.get() + startIndex);
        return currentKey;
    }
}
