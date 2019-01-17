import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author dongliang.wang
 * @createTime 2019/1/9
 **/
public class KafkaConsumer {

    private final ConsumerConnector consumer;

    public KafkaConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect","127.0.0.1:2181");
        properties.put("group.id","wdlgroup");
        properties.put("zookeeper.session.timeout.ms","4000");
        properties.put("zookeeper.connection.timeout.ms","10000");
        properties.put("zookeeper.sync.time.ms","200");
        properties.put("rebalance.max.retries","5");
        properties.put("rebalance.backoff.ms","2000");

        properties.put("auto.commit.interval.ms","1000");
        properties.put("auto.offset.reset","smallest");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(properties);
        consumer = Consumer.createJavaConsumerConnector(config);
    }

    void consumer() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KafkaProducer.TOPIC, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(KafkaProducer.TOPIC).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" + it.next().message() + "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        }
    }

    public static void main(String[] args) {
        new KafkaConsumer().consumer();
    }
}
