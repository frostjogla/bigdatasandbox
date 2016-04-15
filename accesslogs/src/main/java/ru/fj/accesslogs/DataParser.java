package ru.fj.accesslogs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataParser {
    private static final String UNKNOWN_BROWSER = "UNKNOWN";

    private static final String ID = "ID";
    private static final String BYTES = "BYTES";
    private static final String BROWSER = "BROWSER";

    private static final Pattern ID_PATTERN = Pattern.compile("^(?<" + ID + ">(\\w|\\d)+)");
    private static final Pattern BYTES_PATTERN = Pattern
            .compile("(?<" + BYTES + ">(?<=HTTP/\\d.\\d\" \\d\\d\\d )\\d+)");
    private static final Pattern BROWSER_PATTERN = Pattern.compile("(?<" + BROWSER + ">(?<= \"-\" \")\\w+)");

    public Pair<String, String, String> getData(String line) {
        Pair<String, String, String> result = new Pair<>();
        Matcher idMatcher = ID_PATTERN.matcher(line);
        Matcher bytesMatcher = BYTES_PATTERN.matcher(line);
        Matcher browserMatcher = BROWSER_PATTERN.matcher(line);

        result.ip = "null";
        result.bytes = "0";
        result.browser = UNKNOWN_BROWSER;
        if(idMatcher.find()) {
            result.ip = idMatcher.group(ID);
        }
        if (bytesMatcher.find()){
            result.bytes = bytesMatcher.group(BYTES);
        }
        if (browserMatcher.find()){
            result.browser = browserMatcher.group(BROWSER);
        }
        return result;
    }
}
