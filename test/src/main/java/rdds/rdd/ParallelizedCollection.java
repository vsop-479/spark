package rdds.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhouhui on 2018/6/3.
 */
public class ParallelizedCollection {
    public static void main(String[] args){
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir","D:\\hadoop" );
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("parallelizedC");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sparkContext.parallelize(data);

        Integer sum = rdd.reduce((a, b) -> a + b);

        System.out.println(sum);
    }
}
