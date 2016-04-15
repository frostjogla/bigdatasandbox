package ru.fj.sorthdfs;

enum SortOrder {

    DESC(-1), ASC(1);

    private SortOrder(int value) {
        this.value = value;
    }

    final int value;
}
