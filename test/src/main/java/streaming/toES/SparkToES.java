package streaming.toES;

import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class SparkToES {
    public static void main(String[] args) throws InterruptedException {
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkToES");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        User u1 = new User();
        u1.setAge(1);
        u1.setDescription("u2 is 1");
        u1.setName("ZZ");

        User u2 = new User();
        u2.setAge(2);
        u2.setDescription("u2 is 2");
        u2.setName("RR");

        JavaRDD<User> userRDD = jsc.parallelize(ImmutableList.of(u1, u2));
        JavaEsSpark.saveToEs(userRDD, "index/type");
    }
}
