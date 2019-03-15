package streaming.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.Arrays;

public class TransformRDD {
    public static void main(String[] args){
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TransformRDD");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = wordPairs.reduceByKey((v1, v2) -> v1 + v2);

        JavaPairRDD<String, Double> spamInfoRDD = ssc.sparkContext().newAPIHadoopRDD(null, null, null, null);
//        JavaPairDStream<String, Integer> cleanedDStream = wordCounts.transform((a, rdd) -> {
//            rdd.join(spamInfoRDD).filter(...);// join data stream with spam information to do data cleaning
//        });

    }
}
