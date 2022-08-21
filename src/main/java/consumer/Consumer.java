package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 17:09:40
 * @description
 */
public class Consumer {
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

        // 订阅某个主题的特定分区
        // consumer.assign(Arrays.asList(new TopicPartition("test", 0)));

        // 5. 消费消息
        while (IS_RUNNING.get()) {
            // 6. 获取消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            // 6.1 直接打印输出消费的消息
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
            // 6.2 获取消息集中的所有分区
            for (TopicPartition tp : records.partitions()) {
                for (ConsumerRecord<String, String> record : records.records(tp)) {
                    System.out.println(record.partition() + ":" + record.value());
                }
            }
            //if(records.isEmpty()){
            //    IS_RUNNING.set(false);
            //}
        }
    }
}
