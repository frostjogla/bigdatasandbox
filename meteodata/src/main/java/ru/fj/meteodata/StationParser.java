package ru.fj.meteodata;

public class StationParser {

    private final static String DELIMITER = "\t";

    public Station parser(String value) {
        Station station = null;
        if (value != null) {
            String[] split = value.split(DELIMITER);
            station = new Station();
            station.name = split[0];
            station.city = split[1];
        }
        return station;
    }
}
