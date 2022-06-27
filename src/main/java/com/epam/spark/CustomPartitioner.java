package com.epam.spark;

import lombok.RequiredArgsConstructor;
import org.apache.spark.Partitioner;

import static com.epam.util.DataUtils.isShortWord;


@RequiredArgsConstructor
public class CustomPartitioner extends Partitioner {

    private final int numParts;

    @Override
    public int numPartitions() {
        return numParts;
    }

    @Override
    public int getPartition(Object key) {
        if (numParts == 0) {
            return 0;
        }

        final String partitionKey = key.toString();
        return isShortWord(partitionKey) ? 1 % numParts : 2 % numParts;
    }
}
