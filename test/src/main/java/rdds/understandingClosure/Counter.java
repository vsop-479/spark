package rdds.understandingClosure;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Counter {
    public static void main(String[] args) {
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("parallelizedC");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        int counter = 0;
        JavaRDD<Integer> rdd = sparkContext.parallelize(data);
        // Wrong: Don't do this!!
//        rdds.rdd.foreach(x -> counter += x);

//       writing to the executor’s stdout not the driver's.
        rdd.foreach(a -> System.out.println(a));
//        bring the RDD to the driver. fetches the entire RDD
        rdd.collect().forEach(a -> System.out.println(a));
//        bring the RDD to the driver. fetches apart RDD
        rdd.take(100).forEach(a -> System.out.println(a));
        System.out.println("Counter value: " + counter);
    }
}
