package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 17:09:40
 * @description 通过判断assignment的大小来判断，消费者时候获取到了分区
 */
public class ConsumerSeek1 {
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
        // 首先获取分配的分区
        Set<TopicPartition> assignment = new HashSet<>();
        // 先判断是否获取到了分区
        if (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 10);
        }
        // 从尾部开始消费
        /*Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, offsets.get(tp));
        }*/

        //从头部开始消费
        /*Map<TopicPartition, Long> offsets1 = consumer.beginningOffsets(assignment);
        for(TopicPartition tp: assignment){
            consumer.seek(tp, offsets1.get(tp));
        }*/

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
