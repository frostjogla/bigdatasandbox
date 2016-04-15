package ru.fj.sorthdfs;

interface ValueGetter<V, I> {
    V getValue(I input);
}
