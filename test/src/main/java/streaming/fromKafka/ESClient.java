package streaming.fromKafka;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;

public class ESClient {
    public static TransportClient getClient(){
        return Holder.client;
    }

    private static class Holder{
        private static TransportClient client;
        static{
            try {
                Settings setting = Settings.builder()
                        .put("cluster.name", "es")
                        .put("client.transport.sniff", false)
                        .put("client.transport.ping_timeout", "60s")
                        .put("client.transport.nodes_sampler_interval", "60s")
                        .build();
                client = new PreBuiltTransportClient(setting);
                client.addTransportAddress(new TransportAddress(new InetSocketAddress("10.74.17.26",9300)));
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
