package com.epam.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class WordCounterTest {

    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;


    @Before
    public void setUp() {
        final Mapper<LongWritable, Text, Text, IntWritable> mapper = new WordMapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);

        final Reducer<Text, IntWritable, Text, IntWritable> reducer = new WordReducer();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);

        mapReduceDriver = new MapReduceDriver<>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("Two households, both alike in this dignity"));

        mapDriver.withOutput(new Text("two"), new IntWritable(1));
        mapDriver.withOutput(new Text("households,"), new IntWritable(1));
        mapDriver.withOutput(new Text("both"), new IntWritable(1));
        mapDriver.withOutput(new Text("alike"), new IntWritable(1));
        mapDriver.withOutput(new Text("in"), new IntWritable(1));
        mapDriver.withOutput(new Text("dignity"), new IntWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        final List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new Text("Verona"), values);
        reduceDriver.withOutput(new Text("Verona"), new IntWritable(2));

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text("My sword, I say!"));

        mapReduceDriver.addOutput(new Text("my"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("say!"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("sword,"), new IntWritable(1));

        mapReduceDriver.runTest();
    }
}
