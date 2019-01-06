package streaming.tcpDStream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by zhouhui on 2019/1/6.
 */
public class TcpDStream {
    public static void main(String[] args){
//        local[2]:local:a local StreamingContext , 2:two working thread.
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
//        1:batch interval of 1 second.
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

//Create a DStream that will connect to hostname:port, like localhost:9999.
//        lines:the stream of data that will be received from the data server.
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
//Split each line into words.
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//pair:(word, 1).
        JavaPairDStream<String, Integer> wordPairs = words.mapToPair((word) -> new Tuple2<>(word, 1));
//        reduce: v1 + v2.
        JavaPairDStream<String, Integer> wordCounts = wordPairs.reduceByKey((v1, v2) -> v1 + v2);
//        Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

// Start the computation.
        jssc.start();
        try {
            // Wait for the computation to terminate.
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
