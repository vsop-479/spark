package rdds.keyValuePairs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class KeyValuePairs {
    public static void main(String[] args) {
//        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("rdds/wordCount");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//        数据集，划分成行集合lines
        JavaRDD<String> rdd = sparkContext.textFile("D:\\escluster.txt");
//        划分成words集合
        JavaRDD<String> words = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//         未合并的 wordMap:(word, 1)
        JavaPairRDD<String, Integer> wordMap = words.mapToPair(word -> new Tuple2<>(word, 1));
//          count map
        JavaPairRDD<String, Integer> resultMap = wordMap.reduceByKey((v1, v2) -> v1 + v2);
//        distribute on executor's node
        JavaPairRDD<String, Integer> sortedMap = resultMap.sortByKey();
//        bring back to driver node
        List<Tuple2<String, Integer>> collect = sortedMap.collect();
        collect.forEach(a -> System.out.println(a));
        sortedMap.saveAsTextFile("D:\\sparkResult");
    }
}
