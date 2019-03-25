package streaming.checkpoint;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.spark_project.guava.io.Files;
import scala.Tuple2;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class RecoverableNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    private static JavaStreamingContext createContext(String ip, int port, String checkpointDirectory,
                                                      String outputPath){
        System.out.println("Creating new context");
        File outputFile = new File(outputPath);
        if (outputFile.exists()) {
            outputFile.delete();
        }

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("RecoverableNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        ssc.checkpoint(checkpointDirectory);

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(ip, port);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());
        JavaPairDStream<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = wordPairs.reduceByKey((v1, v2) -> v1 + v2);
        wordCounts.checkpoint(Durations.seconds(10));

        wordCounts.foreachRDD((rdd, time) -> {
            Broadcast<List<String>> blacklist = JavaWordBlackList.getInstance(new JavaSparkContext(rdd.context()));
            LongAccumulator droppedWordsCounter =
                    JavaDroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));
            String counts = rdd.filter(wordCount -> {
                if (blacklist.value().contains(wordCount._1())) {
                    droppedWordsCounter.add(wordCount._2());
                    return false;
                } else {
                    return true;
                }
            }).collect().toString();
            String output = "Counts at time " + time + " " + counts;
            System.out.println(output);
            System.out.println("Dropped " + droppedWordsCounter.value() + " word(s) totally");
            System.out.println("Appending to " + outputFile.getAbsolutePath());
            Files.append(output + "\n", outputFile, Charset.defaultCharset());
        });
        return ssc;
    }

    public static void main(String[] args) throws InterruptedException {
        String ip = "localhost";
        int port = 9999;
        String checkpointDirectory = "D:\\spark-space";
        String outputPath = "D:\\spark-result";

        Function0<JavaStreamingContext> createContextFunc = () -> createContext(ip, port, checkpointDirectory, outputPath);
        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);

        ssc.start();
        ssc.awaitTermination();
    }
}

class JavaWordBlackList{
    private static volatile Broadcast<List<String>> instance = null;

    public static Broadcast<List<String>> getInstance(JavaSparkContext jsc){
        if(instance  == null){
            synchronized (JavaWordBlackList.class){
                if(instance == null){
                    List<String> wordBlacklist = Arrays.asList("a", "b", "c");
                    instance = jsc.broadcast(wordBlacklist);
                }
            }
        }
        return instance;
    }
}

class JavaDroppedWordsCounter {
    private static volatile LongAccumulator instance = null;

    public static LongAccumulator getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaDroppedWordsCounter.class) {
                if (instance == null) {
                    instance = jsc.sc().longAccumulator("WordsInBlacklistCounter");
                }
            }
        }
        return instance;
    }
}