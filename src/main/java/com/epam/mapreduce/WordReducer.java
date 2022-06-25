package com.epam.mapreduce;

import lombok.SneakyThrows;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @SneakyThrows
    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
