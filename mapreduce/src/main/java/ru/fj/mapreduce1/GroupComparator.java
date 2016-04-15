package ru.fj.mapreduce1;

import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

    public GroupComparator() {
        super(WordCountPair.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        WordCountPair a1 = (WordCountPair) a;
        WordCountPair b1 = (WordCountPair) b;
        return Long.compare(a1.count, b1.count);
//        return a1.compareTo(b1);
    }
}
