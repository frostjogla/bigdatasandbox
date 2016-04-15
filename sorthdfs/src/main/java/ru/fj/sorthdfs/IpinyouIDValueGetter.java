package ru.fj.sorthdfs;

class IpinyouIDValueGetter implements ValueGetter<String, String> {

    private static final char DELIMITTER = '\t';

    @Override
    public String getValue(String input) {
        int from = input.indexOf(DELIMITTER, input.indexOf(DELIMITTER) + 1);
        int to = input.indexOf(DELIMITTER, from + 1);
        return input.substring(from + 1, to);
    }
}
