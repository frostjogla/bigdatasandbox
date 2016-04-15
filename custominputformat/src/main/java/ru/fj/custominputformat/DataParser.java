package ru.fj.custominputformat;

class DataParser {

    private final String delimiter;

    private String name;
    private Integer age;

    public DataParser(String delimiter) {
        this.delimiter = delimiter;
    }

    void parse(String line) {
        if (line != null) {
            String[] split = line.split(delimiter);
            name = split[0];
            try {
                age = Integer.valueOf(split[1]);
            } catch (NumberFormatException e) {
                age = -1;
                e.printStackTrace(System.err);
            }
        }
    }

    String getName() {
        return name;
    }

    Integer getAge() {
        return age;
    }
}
