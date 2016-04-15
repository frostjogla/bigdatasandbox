package ru.fj.highbidpriced;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {

    String region;
    String city;
    String operationSystem;
    int biddingPrice;

    public CompositeKey() {
    }

    public CompositeKey(String region, String city, String operationSystem, int biddingPrice) {
        this.region = region;
        this.city = city;
        this.operationSystem = operationSystem;
        this.biddingPrice = biddingPrice;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(region != null ? region : "null");
        out.writeUTF(city != null ? city : "null");
        out.writeUTF(operationSystem != null ? operationSystem : "null");
        out.writeInt(biddingPrice);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        region = in.readUTF();
        city = in.readUTF();
        operationSystem = in.readUTF();
        biddingPrice = in.readInt();
    }

    @Override
    public int compareTo(CompositeKey o) {
        return (region + city).compareTo(o.region + o.city);
    }

    @Override
    public int hashCode() {
        return (region + city).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CompositeKey) {
            return (region + city).equals(((CompositeKey) obj).region + ((CompositeKey) obj).city);
        }
        return false;
    }

    @Override
    public String toString() {
        return region + " " + city + " " + operationSystem + " " + biddingPrice;
    }
}
