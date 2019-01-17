import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author dongliang.wang
 * @createTime 2019/1/9
 **/
public class KafkaProducer {

    public final static String TOPIC = "wdl";
    private final Producer<String, String> producer;

    private KafkaProducer() {
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("zk.connect", "127.0.0.1:2181");

        // 配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        props.put("request.required.acks", "-1");

        producer = new Producer<String, String>(new ProducerConfig(props));

    }

    void produce(){
        int messageNo = 1000;
        final int count = 10000;

        while (messageNo < count){
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            producer.send(new KeyedMessage<String, String>(TOPIC,key,data));
            System.out.println(data);
            messageNo++;
        }
    }

    public static void main(String[] args) {
        new KafkaProducer().produce();
    }
}
