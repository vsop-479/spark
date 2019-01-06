package rdds.passingFunctions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class Unwieldy {
    public static void main(String[] args) {
        //        D:/hadoop/bin/winutils.exe
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("RddBasics");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //a external dataset
        JavaRDD<String> lines = sparkContext.textFile("D:\\escluster.txt");
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());

        Integer totalLength = lineLengths.reduce(new Sum() );

        System.out.println(totalLength);
    }
}


class GetLength implements Function<String, Integer> {

    @Override
    public Integer call(String s) throws Exception {
        return s.length();
    }
}

class Sum implements Function2<Integer, Integer, Integer> {

    @Override
    public Integer call(Integer a, Integer b) throws Exception {
        return a + b;
    }
}