package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-07-24 09:11:08
 * @description 第一种多线程消费实现方式
 */
public class MultiConsumerThreadDemo1 {
    public static final String BROKER_LIST = "node1:9092";
    public static final String TOPIC = "threadTest";
    public static final String GROUP_ID = "group.demo";

    public static void main(String[] args) {
        Properties props = initConfig();
        int consumerThreads = 4;
        for (int i = 0; i < consumerThreads; i++) {
            new KafkaConsumerThread(props, TOPIC).start();
        }
    }

    public static Properties initConfig() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return props;
    }

    public static class KafkaConsumerThread extends Thread {
        private final KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties props, String topic) {
            this.kafkaConsumer = new KafkaConsumer<>(props);
            this.kafkaConsumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(Thread.currentThread().getName() +
                                ", partition:" + record.partition() +
                                ", offset:" + record.offset() +
                                ", value:" + record.value() +
                                ", timestamp:" + record.timestamp());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }

    }
}

