package com.epam.spark;

import com.epam.util.DataUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class WordCountRDD {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        final SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final JavaRDD<String> inputFile = sc.textFile(args[0]);
        inputFile.cache();

        final Broadcast<List<String>> stopWords = sc.broadcast(DataUtils.getStopWords());

        final JavaPairRDD<String, Integer> transformedData = inputFile.flatMap(mapToArrayFunction())
                .filter(word -> !stopWords.value().contains(word))
                .map(String::toLowerCase)
                .mapToPair(word -> new Tuple2<>(word, 1));

        final JavaPairRDD<String, Integer> countData = transformedData
                .partitionBy(new CustomPartitioner(2))
                .reduceByKey(Integer::sum);

        countData.take(5).forEach(System.out::println);
        countData.saveAsTextFile("CountDataRDD");

        sc.stop();
    }

    @NotNull
    private static FlatMapFunction<String, String> mapToArrayFunction() {
        return content -> Arrays.asList(content.split(" ")).iterator();
    }
}
