import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 16:54:03
 * @description 创建一个Kafka生产者，并同步发送数据，只有数据全部发送成功之后，才会进行下一步动作
 */
public class ProducerSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
            // 在send方法之后加上get()方法，会等待所有的消息发送成功之后，才会进行下一步动作
            producer.send(new ProducerRecord<String, String>("test", "hello" + i)).get();
        }
        // 6. 关闭生产者对象
        producer.close();


    }
}
