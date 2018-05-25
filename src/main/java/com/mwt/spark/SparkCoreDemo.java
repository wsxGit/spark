package com.mwt.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkCoreDemo {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("aaa");
        sparkConf.setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRDD = javaSparkContext.textFile("e:/a.txt");
        JavaRDD<String> wordsRDD = javaRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapRDD = wordsRDD.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairRDD<String, Integer> resultRDD = mapRDD.reduceByKey((v1, v2) -> v1 + v2);
        resultRDD.foreach(x-> System.out.println(x._1+":"+x._2));


    }
}
