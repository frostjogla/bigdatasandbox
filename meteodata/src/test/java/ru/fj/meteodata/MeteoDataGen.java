package ru.fj.meteodata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class MeteoDataGen {

    private static final char DELIMITER = '\t';
    private final static int START_WEATHER_OBSERVATIONS = 1900;
    private final static String[] CITYS = { "Saint-Petersburg", "Moscow", "Minsk", "Kiev", "Omsk", "Kazan", "Samara" };

    private NameGenerator stationGenerator;
    private TemperatureGenerator temperatureGenerator;

    @Before
    public void setUp() {
        stationGenerator = new NameGenerator();
        temperatureGenerator = new TemperatureGenerator();
    }

    @Test
    public void genTemperatureAndStationData() throws Exception {

        Set<String> stations = new HashSet<>();
        List<String> dataList = new ArrayList<>(156);

        for (int i = 100; i >= 0; i--) {
            String stationName = stationGenerator.generateName();
            stations.add(stationName);
            for (int ii = 0; ii < 50; ii++) {
                dataList.add(new StringBuilder().append(stationName).append(DELIMITER)
                        .append(temperatureGenerator.generateTemperature()).append(DELIMITER)
                        .append(START_WEATHER_OBSERVATIONS + ii).toString());
            }
        }
        Collections.shuffle(dataList);
        dataList.forEach((data) -> System.out.println(data));
        System.out.println("==================");
        dataList.clear();
        int i = 0;
        for (String station : stations) {
            dataList.add(
                    new StringBuilder().append(station).append(DELIMITER).append(CITYS[i % CITYS.length]).toString());
            i++;
        }
        Collections.shuffle(dataList);
        dataList.forEach((data) -> System.out.println(data));
    }
}
