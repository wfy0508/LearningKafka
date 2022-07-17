package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 17:09:40
 * @description 通过seek重置消费位移
 */
public class ConsumerSeek {
    public static final AtomicBoolean IS_RUNNING = new AtomicBoolean(true);

    public static void main(String[] args) {
        // 1. 创建消费者配置对象
        Properties props = new Properties();
        // 2. 配置bootstrap.servers属性
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 3. 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 4. 订阅主题
        consumer.subscribe(Collections.singletonList("test"));
        consumer.poll(Duration.ofMillis(1000));
        // 首先获取分配的分区
        Set<TopicPartition> assignment = consumer.assignment();
        for (TopicPartition tp : assignment) {
            // 从获得的分区，重置消费位移为10
            consumer.seek(tp, 500);
        }
        while (IS_RUNNING.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("partition: " + record.partition() + ", " +
                        "offset: " + record.offset() + ", " +
                        "key: " + record.key() + ", " +
                        "value: " + record.value());
            }
            if (records.isEmpty()) {
                IS_RUNNING.set(false);
            }
        }
    }
}
