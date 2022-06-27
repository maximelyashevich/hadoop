package com.epam.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import static com.epam.util.DataUtils.isShortWord;


public class WordPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(final Text key, final IntWritable value, final int numPartitions) {
        if (numPartitions == 0) {
            return 0;
        }

        final String partitionKey = key.toString();
        return isShortWord(partitionKey) ? 1 % numPartitions : 2 % numPartitions;
    }
}
