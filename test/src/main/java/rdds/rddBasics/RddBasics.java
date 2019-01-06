package rdds.rddBasics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class RddBasics {
    public static void main(String[] args) {
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("RddBasics");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //a external dataset
        JavaRDD<String> lines = sparkContext.textFile("D:\\escluster.txt");
        //transformation to a new dataset
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        //persist lineLengths
        lineLengths.persist(StorageLevel.MEMORY_ONLY());
        //action to a value
        Integer totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(totalLength);
    }
}
