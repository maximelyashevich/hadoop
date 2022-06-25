package com.epam.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordPartitioner extends Partitioner<Text, IntWritable> {

    private static final int MIN_WORD_LENGTH = 5;

    @Override
    public int getPartition(final Text key, final IntWritable value, final int numPartitions) {
        if (numPartitions == 0) {
            return 0;
        }

        final String partitionKey = key.toString();
        return isShortWord(partitionKey) ? 1 % numPartitions : 2 % numPartitions;
    }

    private static boolean isShortWord(final String partitionKey) {
        return StringUtils.length(partitionKey) < MIN_WORD_LENGTH;
    }
}
