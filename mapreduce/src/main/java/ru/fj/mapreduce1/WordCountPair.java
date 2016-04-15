package ru.fj.mapreduce1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class WordCountPair implements WritableComparable<WordCountPair> {

    String word;
    long count;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        count = in.readLong();
    }

    @Override
    public int compareTo(WordCountPair o) {
        return word.compareTo(o.word);
    }

    @Override
    public boolean equals(Object obj) {
        WordCountPair pair = (WordCountPair) obj;
        return (this == pair) || Objects.equals(word, pair.word);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((word == null) ? 0 : word.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return count + " " + word;
    }
}
