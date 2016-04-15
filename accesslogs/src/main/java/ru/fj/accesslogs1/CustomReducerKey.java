package ru.fj.accesslogs1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomReducerKey implements WritableComparable<CustomReducerKey> {

    String ip;
    double avgBytes;
    long totalBytes;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ip);
        out.writeDouble(avgBytes);
        out.writeLong(totalBytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ip = in.readUTF();
        avgBytes = in.readDouble();
        totalBytes = in.readLong();
    }

    @Override
    public int compareTo(CustomReducerKey o) {
        return ip.compareTo(o.ip);
    }

}
