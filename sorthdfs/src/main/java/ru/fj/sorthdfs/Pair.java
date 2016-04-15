package ru.fj.sorthdfs;

class Pair {

    public Pair() {
    }

    public Pair(String key, int value) {
        this.key = key;
        this.value = value;
    }

    String key;
    int value;

    @Override
    public String toString() {
        return key + "\t" + value;
    }
}
