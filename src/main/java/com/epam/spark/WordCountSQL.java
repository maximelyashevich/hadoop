package com.epam.spark;

import com.epam.util.DataUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import scala.reflect.ClassManifestFactory;

import java.util.Arrays;
import java.util.List;


public class WordCountSQL {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        final SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local")
                .config("spark.hadoop.mapreduce.output.fileoutputformat.compress", false)
                .getOrCreate();

        final Dataset<String> inputFile = spark.read().textFile(args[0]);
        inputFile.cache();

        final Broadcast<List<String>> stopWords = spark.sparkContext().broadcast(
                DataUtils.getStopWords(),
                ClassManifestFactory.classType(List.class)
        );

        final Dataset<Row> transformedData = inputFile.flatMap(mapToArrayFunction(), Encoders.STRING())
                .filter((FilterFunction<String>) word -> !stopWords.value().contains(word))
                .map((MapFunction<String, String>) String::toLowerCase, Encoders.STRING())
                .toDF("word");
        transformedData.createOrReplaceTempView("words");

        final Dataset<Row> result = transformedData.sqlContext().sql("SELECT word, count(*) FROM words GROUP BY word");

        result.limit(5).show();
        result.repartition(1).write().csv("CountDataSQL");

        spark.stop();
    }

    @NotNull
    private static FlatMapFunction<String, String> mapToArrayFunction() {
        return content -> Arrays.asList(content.split(" ")).iterator();
    }
}
