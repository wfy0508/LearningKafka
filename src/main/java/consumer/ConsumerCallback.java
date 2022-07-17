package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 17:09:40
 * @description
 */
public class ConsumerCallback {
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
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        while (IS_RUNNING.get()) {
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.out.println(offsets);
                    } else {
                        System.out.println("fail to commit offset!");
                    }
                }
            });
        }

    }
}
