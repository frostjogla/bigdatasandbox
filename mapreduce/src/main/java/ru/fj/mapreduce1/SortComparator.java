package ru.fj.mapreduce1;

import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

    public SortComparator() {
        super(WordCountPair.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        WordCountPair p1 = (WordCountPair) a;
        WordCountPair p2 = (WordCountPair) b;
        return Long.compare(p1.count, p2.count);
    }
}
