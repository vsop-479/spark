package streaming.dataFrame;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.regex.Pattern;

public class JavaSqlNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\hadoop");

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaSqlNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        String hostname = "localhost";
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostname, 9999);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());

        words.foreachRDD((rdd, time) -> {
            // Get the singleton instance of SparkSession
            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

            // Convert RDD[String] to RDD[case class] to DataFrame
            JavaRDD<JavaRecord> rowRDD = rdd.map(word -> {
                JavaRecord record = new JavaRecord();
                record.setWord(word);
                return record;
            });

            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRecord.class);

            // Creates a temporary view using the DataFrame
            wordsDataFrame.createOrReplaceTempView("words");

            // Do word count on table using SQL and print it
            Dataset<Row> wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word");
            System.out.println("========= " + time + "=========");
            wordCountsDataFrame.show();
        });

        ssc.start();
        ssc.awaitTermination();
    }
}

class JavaSparkSessionSingleton{
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf){
        if(instance == null){
            instance = SparkSession.builder().config(sparkConf).getOrCreate();
        }
        return instance;
    }
}

class JavaRecord{
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}