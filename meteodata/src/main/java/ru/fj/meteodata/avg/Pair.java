package ru.fj.meteodata.avg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class Pair implements Writable {

    int sum;
    int amount;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
        amount = in.readInt();
    }

    public double avg() {
        return (double) sum / (double) amount;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + amount;
        result = prime * result + sum;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Pair other = (Pair) obj;
        if (amount != other.amount)
            return false;
        if (sum != other.sum)
            return false;
        return true;
    }
}
