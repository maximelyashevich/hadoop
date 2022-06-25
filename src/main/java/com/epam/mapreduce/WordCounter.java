package com.epam.mapreduce;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCounter extends Configured implements Tool {

    @SneakyThrows
    public int run(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: WordCounter <input path> <output path");
            System.exit(-1);
        }

        // Define MapReduce job
        final Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCounter.class);
        job.setJobName("WordCounter");

        // Set input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(WordReducer.class);
        job.setPartitionerClass(WordPartitioner.class);
        job.setNumReduceTasks(2);
        job.setReducerClass(WordReducer.class);

        // Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @SneakyThrows
    public static void main(String[] args) {
        final int result = ToolRunner.run(new Configuration(), new WordCounter(), args);
        System.exit(result);
    }
}