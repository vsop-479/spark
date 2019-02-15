package streaming.fileDStream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class FileDStream {

    public static void main(String[] args){
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        //        local[2]:local:a local StreamingContext , 2:two working thread.
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        //        1:batch interval of 1 second.
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        String directory = "E:\\problem\\connect";
        JavaDStream<String> lines = streamingContext.textFileStream(directory);
        //Split each line into words.
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//pair:(word, 1).
        JavaPairDStream<String, Integer> wordPairs = words.mapToPair((word) -> new Tuple2<>(word, 1));
//        reduce: v1 + v2.
        JavaPairDStream<String, Integer> wordCounts = wordPairs.reduceByKey((v1, v2) -> v1 + v2);
//        Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

// Start the computation.
        streamingContext.start();
        try {
            // Wait for the computation to terminate.
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
