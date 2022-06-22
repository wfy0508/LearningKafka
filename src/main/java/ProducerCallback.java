import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 16:54:03
 * @description 创建一个Kafka生产者，并异步发送数据
 */
public class ProducerCallback {
    public static void main(String[] args) {
        // 1. 创建生产者配置对象
        Properties props = new Properties();
        // 2. 配置bootstrap.servers属性
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        // 3. 配置key, value序列化属性
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 4. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        // 5. 调用生产者对象的send方法发送消息
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<String, String>("test", "hello" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("主题: " + metadata.topic() + " 分区: " + metadata.partition() + " 偏移量: " + metadata.offset());
                    }
                }
            });
        }
        // 6. 关闭生产者对象
        producer.close();


    }
}
