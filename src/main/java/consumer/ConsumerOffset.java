package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-07-16 13:53:40
 * @description lastConsumedOffset、committed offset和position之间的关系
 */
public class ConsumerOffset {
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
        TopicPartition tp = new TopicPartition("test", 0);
        consumer.assign(Collections.singletonList(tp));
        // 当前消费到的位移
        long lastConsumerOffset = -1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                break;
            }
            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
            lastConsumerOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            // 同步提交消费位移，整个消息集同步提交
            consumer.commitAsync();
        }
        System.out.println("consumed offset is " + lastConsumerOffset);
        OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
        System.out.println("committed offset is " + offsetAndMetadata.offset());
        long position = consumer.position(tp);
        System.out.println("the offset of the next record is " + position);
    }
}
