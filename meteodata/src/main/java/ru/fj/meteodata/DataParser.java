package ru.fj.meteodata;

public class DataParser {

    private final static String DELIMITER = "\t";

    public TemperatureData parse(String value) {
        TemperatureData result = null;
        if (value != null) {
            result = new TemperatureData();
            String[] split = value.split(DELIMITER);
            result.stationName = split[0];
            result.temperature = Integer.valueOf(split[1]);
            result.year = Integer.valueOf(split[2]);
        }
        return result;
    }
}
