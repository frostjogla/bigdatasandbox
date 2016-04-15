package ru.fj.meteodata.avg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

class DecadeYear implements WritableComparable<DecadeYear> {

    int year;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year / 10 * 10);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt() / 10 * 10;
    }

    @Override
    public int compareTo(DecadeYear o) {
        return Integer.compare(year, o.year);
    }

    @Override
    public String toString() {
        return new StringBuilder().append(year).toString();
    }
}
