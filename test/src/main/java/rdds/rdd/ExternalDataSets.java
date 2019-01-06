package rdds.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zhouhui on 2018/6/3.
 */
public class ExternalDataSets {
    public static void main(String[] args) {
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("parallelizedC");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("D:\\escluster.txt");

        Integer fileLength = rdd.map(s -> s.length()).reduce((a, b) -> a + b);
        System.out.println(fileLength);
    }
}
