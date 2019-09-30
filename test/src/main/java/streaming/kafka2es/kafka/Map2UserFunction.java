package streaming.kafka2es.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import streaming.kafka2es.po.User;
import streaming.kafka2es.util.GsonUtil;

public class Map2UserFunction implements Function<ConsumerRecord<String, String>, User> {
    @Override
    public User call(ConsumerRecord<String, String> record) throws Exception {
        return GsonUtil.getGson().fromJson(record.value(), User.class);
    }
}
