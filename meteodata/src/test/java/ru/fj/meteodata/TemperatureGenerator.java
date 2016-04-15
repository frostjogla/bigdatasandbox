package ru.fj.meteodata;

import java.util.Random;

class TemperatureGenerator {

    private final static Random RANDOM = new Random();
    private final static int MAX_TEMPERATURE = 75;

    int generateTemperature() {
        return MAX_TEMPERATURE - RANDOM.nextInt(MAX_TEMPERATURE * 2);
    }
}
