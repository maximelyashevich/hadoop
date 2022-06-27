package com.epam.util;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@UtilityClass
public final class DataUtils {

    private static final int MIN_WORD_LENGTH = 5;

    private static final Set<String> stopWords = new HashSet<>();

    static {
        stopWords.addAll(
                Arrays.asList("I", "a", "about", "as", "new", "this", "to", "what", "when", "where", "who")
        );
    }

    public static boolean isStopWord(final String data) {
        return stopWords.stream().anyMatch(data::equalsIgnoreCase);
    }

    public static List<String> getStopWords() {
        return new ArrayList<>(DataUtils.stopWords);
    }

    public static boolean isShortWord(final String partitionKey) {
        return StringUtils.length(partitionKey) < MIN_WORD_LENGTH;
    }
}
