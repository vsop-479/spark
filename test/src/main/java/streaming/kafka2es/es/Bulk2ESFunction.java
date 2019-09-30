package streaming.kafka2es.es;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import streaming.kafka2es.po.User;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Bulk2ESFunction implements VoidFunction<JavaRDD<User>> {
    @Override
    public void call(JavaRDD<User> userJavaRDD) throws Exception {
        userJavaRDD.foreachPartition(new VoidFunction<Iterator<User>>() {
            @Override
            public void call(Iterator<User> userIterator) throws Exception {
                TransportClient client = ESClient.getClient();
                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                Map<String, Object> map = new HashMap<>();
                while(userIterator.hasNext()){
                    User user = userIterator.next();
                    map.put("name", user.getName());
                    map.put("age", user.getAge());
                    map.put("desc", user.getDescription());
                    IndexRequest request = client.prepareIndex("users", "info").setSource(map).request();
                    bulkRequestBuilder.add(request);
                }
                if(bulkRequestBuilder.numberOfActions() > 0){
                    BulkResponse bulkItemResponses = bulkRequestBuilder.execute().actionGet();
                }
            }
        });
    }
}
