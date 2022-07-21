package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 17:09:40
 * @description
 */
public class ConsumerExpireRecords {
    public static final AtomicBoolean IS_RUNNING = new AtomicBoolean(true);

    public static void main(String[] args) {
        // 1. 创建消费者配置对象
        Properties props = new Properties();
        // 2. 配置bootstrap.servers属性
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置超时消息拦截器
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptor.ConsumerInterceptorTest.class.getName());
        // 3. 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 4. 订阅主题
        consumer.subscribe(Collections.singletonList("test"));

        while (IS_RUNNING.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1000));
            for (TopicPartition tp : records.partitions()) {
                for (ConsumerRecord<String, String> record : records.records(tp)) {
                    System.out.println("topic: " + record.topic() +
                            ", partition: " + record.partition() +
                            ", key: " + record.key() +
                            ", value: " + record.value()+
                            "， offset: "+record.offset());
                }
            }
        }
    }
}
