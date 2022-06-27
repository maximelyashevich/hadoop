package com.epam.spark_sms;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.Arrays;


public class SpamService {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        final SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Spam service");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final JavaPairRDD<String, String> inputData = sc.textFile(args[0])
                .mapToPair(mapToPairFunction())
                .cache();

        final JavaRDD<String> hamWords = inputData.filter(pair -> "ham".equals(pair._1))
                .flatMap(getWordsFunction())
                .distinct()
                .cache();

        final JavaRDD<Tuple2<String, Integer>> spamWords = inputData.filter(pair -> !"ham".equals(pair._1))
                .flatMap(getWordsFunction())
                .subtract(hamWords)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .map(x -> new Tuple2<>(x._1, x._2))
                .sortBy(x -> x._2, false, 1);

        spamWords.take(5).forEach(System.out::println);
        spamWords.saveAsTextFile("SpamService");

        sc.stop();
    }

    @NotNull
    private static FlatMapFunction<Tuple2<String, String>, String> getWordsFunction() {
        return tuple2 -> Arrays.stream(tuple2._2.split(" ")).iterator();
    }

    @NotNull
    private static PairFunction<String, String, String> mapToPairFunction() {
        return line -> {
            final String[] values = line.split("\t");
            return new Tuple2<>(values[0], values[1]);
        };
    }
}
