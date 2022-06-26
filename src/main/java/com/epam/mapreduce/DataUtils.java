package com.epam.mapreduce;

import lombok.Getter;
import lombok.experimental.UtilityClass;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


@UtilityClass
public final class DataUtils {

    @Getter
    private static final Set<String> stopWords = new HashSet<>();

    static {
        stopWords.addAll(
                Arrays.asList("I", "a", "about", "as", "this", "to", "what", "when", "where", "who")
        );
    }

    public static boolean isStopWord(final String data) {
        return stopWords.stream().anyMatch(data::equalsIgnoreCase);
    }
}
