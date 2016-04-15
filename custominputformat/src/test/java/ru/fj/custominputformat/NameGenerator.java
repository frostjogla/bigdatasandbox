package ru.fj.custominputformat;

import java.util.Random;

class NameGenerator {

    private final static int MAX_NAME_LENGTH = 7;
    private final static int MIN_NAME_LENGTH = 4;

    private final static Random RANDOM = new Random();

    private final static String A_G = "ABCDEFG";

    String generateName() {
        StringBuilder sb = new StringBuilder();
        int nameLenth = RANDOM.nextInt(MAX_NAME_LENGTH);
        nameLenth = nameLenth > 3 ? nameLenth : MIN_NAME_LENGTH;
        for (int i = 0; i < nameLenth; i++) {
            sb.append(A_G.charAt(RANDOM.nextInt(A_G.length())));
        }
        return sb.toString();
    }

}
