package streaming.output.foreachRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SendRecord {
    public static void main(String[] args){
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SendRecord");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
//        每一个record都要创建connection.
        lines.foreachRDD(rdd -> {
            rdd.foreach(record -> {
//                Connection connection = createNewConnection();
//                connection.send(record);
//                connection.close();
            });
        });

//        同一个partition的records使用一个connection.
        lines.foreachRDD(rdd ->{
            rdd.foreachPartition(records -> {
//                Connection connection = createNewConnection();
                while (records.hasNext()){
//                    connection.send(partitionOfRecords.next());
                }
//                connection.close();
            });
        });

//        使用连接池.
        lines.foreachRDD(rdd ->{
            rdd.foreachPartition(records -> {
//                Connection connection = ConnectionPool.getConnection();
                while (records.hasNext()){
//                    connection.send(partitionOfRecords.next());
                }
//                connection.close();
            });
        });
    }
}
