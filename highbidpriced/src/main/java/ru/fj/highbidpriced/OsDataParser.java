package ru.fj.highbidpriced;

class OsDataParser {

    String getOperationSystem(String inStr) {
        if (inStr != null) {
            if (inStr.contains("Linux") && !inStr.contains("Android")) {
                return OperationSystem.Linux.name();
            } else if (inStr.contains("Linux") && inStr.contains("Android")) {
                return OperationSystem.Android.name();
            } else if (inStr.contains("Windows")) {
                return OperationSystem.Windows.name();
            } else if (inStr.contains("Mac OS X")) {
                return OperationSystem.MacOs.name();
            } else {
                return OperationSystem.Other.name();
            }
        } else {
            return OperationSystem.Other.name();
        }
    }

    private static enum OperationSystem {
        MacOs, Android, Windows, Linux, Other
    }

}
