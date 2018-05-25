package com.mwt.spark;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSqlDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "wangsixian");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:mysql://localhost:3306/wsx", "t_test", connectionProperties);

        jdbcDF2.printSchema();
        jdbcDF2.select("id","name").show();
    }
}
