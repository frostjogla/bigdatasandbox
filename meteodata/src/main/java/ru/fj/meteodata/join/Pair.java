package ru.fj.meteodata.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Pair implements WritableComparable<Pair> {

    String first;
    String second;

    public Pair() {
    }

    public Pair(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeUTF(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readUTF();
        second = in.readUTF();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pair) {
            Pair other = (Pair) obj;
            return Objects.equals(first, other.first) && Objects.equals(second, other.second);
        }
        return false;
    }

    @Override
    public int compareTo(Pair o) {
        int result = first.compareTo(o.first);
        if (result != 0) {
            return result;
        }
        return second.compareTo(o.second);
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    public static class FirstComparator extends WritableComparator {

        public FirstComparator() {
            super(Pair.class, true);
        }

        @Override
        public int compare(@SuppressWarnings("rawtypes") WritableComparable a,
                @SuppressWarnings("rawtypes") WritableComparable b) {
            Pair a1 = (Pair) a;
            Pair b1 = (Pair) b;
            return a1.first.compareTo(b1.first);
        }
    }
}
