package ru.fj.meteodata;

import java.util.Random;

class NameGenerator {

    private final static int MAX_NAME_LENGTH = 4;
    private final static int MIN_NAME_LENGTH = 4;

    private final static Random RANDOM = new Random();

    private final static String A_D = "ABCD";

    String generateName() {
        StringBuilder sb = new StringBuilder();
        int nameLenth = RANDOM.nextInt(MAX_NAME_LENGTH);
        nameLenth = nameLenth > 3 ? nameLenth : MIN_NAME_LENGTH;
        for (int i = 0; i < nameLenth; i++) {
            sb.append(A_D.charAt(RANDOM.nextInt(A_D.length())));
        }
        return sb.toString();
    }

}
