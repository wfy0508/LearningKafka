package consumer;

import jdk.nashorn.internal.codegen.ClassEmitter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-07-16 13:53:40
 * @description 同步批量提交
 */
public class ConsumerSyncBatch {
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
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        // 批量提交
        while (IS_RUNNING.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() > minBatchSize) {
                // 逻辑处理
                System.out.println("缓冲区大小为：" + buffer.size());
                consumer.commitSync();
                buffer.clear();
            }
        }
        // commitSync带参数的同步位移提交
        /*while (IS_RUNNING.get()) {
            ConsumerRecords<String, String> records1 = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records1) {
                long offset = record.offset();
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
            }
        }*/

        try {
            while (IS_RUNNING.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        // do something
                        System.out.println(record.toString());
                    }
                    long lastConsumerOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumerOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
}
