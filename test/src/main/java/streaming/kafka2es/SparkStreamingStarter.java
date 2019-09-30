package streaming.kafka2es;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import streaming.kafka2es.es.Bulk2ESFunction;
import streaming.kafka2es.kafka.Map2UserFunction;
import streaming.kafka2es.po.User;
import java.util.*;

public class SparkStreamingStarter {
    public static void main(String[] args){
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SendRecord");
        conf.set("spark.streaming.backpressure.enabled", "true");
        conf.set("spark.streaming.receiver.maxRate", "1000");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "1000");
        conf.set("es.nodes", "10.74.17.26");
        conf.set("es.port", "9200");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(2));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.95.134.105:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "sparkGroup4");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("users");
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream
                (ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaDStream<User> kafkaDStream = stream.map(new Map2UserFunction());

        kafkaDStream.foreachRDD(new Bulk2ESFunction());
        ssc.start();

        try {
            // Wait for the computation to terminate.
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
