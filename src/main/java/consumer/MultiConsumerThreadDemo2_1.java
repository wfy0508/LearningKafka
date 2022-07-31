package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-07-24 09:11:08
 * @description 第二种多线程消费实现方式：多线程处理消息(RecordHandler中run使用同步方法，防止出现并发问题)
 */
public class MultiConsumerThreadDemo2_1 {
    public static final String BROKER_LIST = "node1:9092";
    public static final String TOPIC = "threadTest";
    public static final String GROUP_ID = "group.demo";

    public static void main(String[] args) {
        Properties props = initConfig();
        // 使用可用的CPU核数来并发执行
        new KafkaConsumerThread1(props, TOPIC, Runtime.getRuntime().availableProcessors()).start();
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

    public static class KafkaConsumerThread1 extends Thread {
        private final KafkaConsumer<String, String> kafkaConsumer;
        private final ExecutorService executorService;
        private final int threadNumber;

        public KafkaConsumerThread1(Properties props, String topic, int threadNum) {
            kafkaConsumer = new KafkaConsumer<>(props);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            this.threadNumber = threadNum;
            // 通过线程池来并行处理消息
            executorService = new ThreadPoolExecutor(threadNumber,
                    threadNumber,
                    0,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
        }

        @Override
        public void run() {
            try {
                while (true) {
                    final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordHandler(records));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }

    /**
     * 多线程处理消息
     */
    public static class RecordHandler extends Thread {
        public final ConsumerRecords<String, String> records;

        public RecordHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            for (TopicPartition tp : records.partitions()) {
                final List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
                // 获取消息的最后offset的位置
                final long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(100);
                synchronized (offsets) {
                    if (!offsets.containsKey(tp)) {
                        offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                    } else {
                        long position = offsets.get(tp).offset();
                        if (position < lastConsumedOffset + 1) {
                            offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                        }
                    }
                }
            }

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(Thread.currentThread().getName() +
                        ", partition:" + record.partition() +
                        ", offset:" + record.offset() +
                        ", value:" + record.value() +
                        ", timestamp:" + record.timestamp());
            }
        }
    }

}

