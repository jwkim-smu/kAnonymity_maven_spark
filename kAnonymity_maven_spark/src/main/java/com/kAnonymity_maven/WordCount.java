package com.kAnonymity_maven;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class WordCount implements Serializable {
    public void execute(String inputPath, String outputFile) {

        SparkConf conf1 = new SparkConf().setMaster("yarn-cluster").setAppName("WordCount");
        JavaSparkContext sc1 = new JavaSparkContext(conf1);

        JavaRDD<String> input = sc1.textFile(inputPath).cache();
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {

            /// https://stackoverflow.com/questions/38880956/spark-2-0-0-arrays-aslist-not-working-incompatible-types/38881118#38881118
            /// In 2.0, FlatMapFunction.call() returns an Iterator rather than Iterable. Try this:
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
            }
        }).repartition(16);
        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String x) {
                return new Tuple2(x, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });

        
        System.out.println(counts.collect());
        
        counts.saveAsTextFile(outputFile);
        sc1.stop();
    }

    public static void main( String[] args ) {
    	//System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-2.8.4");
        String inputFile = args[0];       /// args[0];
        String outputFile = args[1];     /// args[1];
        WordCount wc = new WordCount();
        wc.execute(inputFile, outputFile);
    }
}


