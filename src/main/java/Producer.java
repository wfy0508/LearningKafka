import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 16:54:03
 * @description 创建一个Kafka生产者
 */
public class Producer {
    public static void main(String[] args) {
        // 1. 创建生产者配置对象
        Properties props = new Properties();
        // 2. 配置bootstrap.servers属性
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        // 3. 配置key, value序列化属性
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        // 拦截器(多个拦截器用逗号隔开)
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ProducerInterceptorPrefix.class.getName() +
                "," +
                ProducerInterceptorPlus.class.getName());

        // 4. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        // 5. 调用生产者对象的send方法发送消息
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<String, String>("test", "hello" + i));
        }
        // 6. 关闭生产者对象
        producer.close();


    }
}
