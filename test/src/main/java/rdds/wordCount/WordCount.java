package rdds.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by zhouhui on 2018/4/6.
 * wordCount三步骤:
 * 1:读文件生成行rdd.
 * 2:对每行数据分词成单词rdd.
 * 3:map:对单词rdd处理，形成key为单词，value为数量1的元组集pairRDD.
 * 4:reduce:对元组集pairRDD按照key做reduce, 相同的key合并, value为和.
 */
public class WordCount {
    public static void main(String[] args){
//        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir","D:\\hadoop" );
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("rdds/wordCount");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//        数据集，划分成行集合lines
        JavaRDD<String> rdd = sparkContext.textFile("D:\\escluster.txt");
        long count = rdd.count();
        String first = rdd.first();
//        分成单词集合
        JavaRDD<String> words =
                rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair((w) -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((c1, c2) -> c1 + c2);
        counts.saveAsTextFile("D:\\sparkResult");

    }
}
