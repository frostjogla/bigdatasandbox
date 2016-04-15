package ru.fj.mapreduce;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class SplitWord {

    private final static Pattern WORD_PATTERN = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);

    @Test
    public void splitTextByWord() {
        String value = "  private final static Pattern WORD_PATTERN = Ах Pattern.compile(); a фывджао  фывджоа ж 8 * фывжа"
                + "jfasl; 884235  4 52345ohasjkldf 9-345r kjdhl Ах Ах";
        Matcher matcher = WORD_PATTERN.matcher(value);
        while (matcher.find()) {
            String group = matcher.group();
            System.out.println(group);
        }
    }
}
