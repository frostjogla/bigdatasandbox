package ru.fj.yarnapp;

public class MyAppMaster {

    public static void main(String[] args) throws Exception {
        EmbeddedJettyStarter jettyStarter = new EmbeddedJettyStarter();
        jettyStarter.start();
    }

}
