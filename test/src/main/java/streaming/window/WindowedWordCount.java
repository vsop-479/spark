package streaming.window;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WindowedWordCount {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WindowedWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        // Reduce last 30 seconds of data, every 10 seconds
        JavaPairDStream<String, Integer> windowedWordCounts =
                wordPairs.reduceByKeyAndWindow((v1, v2) -> v1 + v2, Durations.seconds(30), Durations.seconds(10));

        windowedWordCounts.print();

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
