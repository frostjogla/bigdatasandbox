package ru.fj.custominputformat;

import java.util.Random;

class AgeGenerator {

    private final static Random RANDOM = new Random();
    private final static int MAX_AGE = 120;

    int generateAge() {
        return MAX_AGE - RANDOM.nextInt(MAX_AGE);
    }
}
