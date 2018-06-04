package passingFunctions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class Anonymous {
    public static void main(String[] args) {
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("RddBasics");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //a external dataset
        JavaRDD<String> lines = sparkContext.textFile("D:\\escluster.txt");
        JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        Integer totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        System.out.println(totalLength);
    }
}