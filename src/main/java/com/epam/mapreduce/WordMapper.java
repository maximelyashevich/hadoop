package com.epam.mapreduce;

import lombok.SneakyThrows;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.StringTokenizer;

import static com.epam.util.DataUtils.isStopWord;


public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);

    private final Text word = new Text();

    @SneakyThrows
    @Override
    public void map(final LongWritable key, final Text value, final Context context) {
        final String line = value.toString();
        final StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().toLowerCase());
            if (isStopWord(word.toString())) {
                continue;
            }
            context.write(word, one);
        }
    }
}
