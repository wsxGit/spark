package com.mwt.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class KafkaSparkSreamingDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("streaming word count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
        ssc.checkpoint("e:/kafka_checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("wordcount");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> stringJavaDStream = stream.flatMap(record -> Arrays.asList(record.value().split(" ")).iterator());
        JavaPairDStream<String, Integer> stringStringJavaPairDStream = stringJavaDStream.mapToPair(record -> new Tuple2<>(record, 1));


        JavaPairDStream<String, Integer> wordcounts = stringStringJavaPairDStream.updateStateByKey(

                // 这里的Optional,相当于scala中的样例类,就是Option,可以理解它代表一个状态,可能之前存在,也可能之前不存在
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                    private static final long serialVersionUID = 1L;

                    // 实际上,对于每个单词,每次batch计算的时候,都会调用这个函数,第一个参数,values相当于这个batch中,这个key的新的值,
                    // 可能有多个,比如一个hello,可能有2个1,(hello,1) (hello,1) 那么传入的是(1,1)
                    // 那么第二个参数表示的是这个key之前的状态,其实泛型的参数是你自己指定的
                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        // 定义一个全局的计数
                        Integer newValue = 0;
                        if (state.isPresent()) {
                            newValue = state.get();
                        }
                        for (Integer value : values) {
                            newValue += value;
                        }
                        return Optional.of(newValue);
                    }
                });

        wordcounts.print();
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
}
