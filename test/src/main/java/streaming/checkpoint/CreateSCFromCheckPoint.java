package streaming.checkpoint;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class CreateSCFromCheckPoint {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("CreateSCFromCheckPoint");

        JavaStreamingContext context = JavaStreamingContext.getOrCreate("D:\\spark-space", () -> {
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

            List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
            JavaPairRDD<String, Integer> initialRDD = jssc.sparkContext().parallelizePairs(tuples);

            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
            JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());
            JavaPairDStream<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));

            Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                    (word, one, state) -> {
                        int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                        Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                        state.update(sum);
                        return output;
                    };

            JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDStream =
                    wordPairs.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

            stateDStream.print();
            return jssc;
        });

        context.start();
        context.awaitTermination();
    }
}
