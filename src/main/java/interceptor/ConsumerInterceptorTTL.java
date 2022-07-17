package interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-07-17 16:33:05
 * @description 消费者拦截器，实现超时消息处理
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        // newRecords用于存放各个分区超时的消息，带分区信息
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>(100);
        for (TopicPartition tp : records.partitions()) {
            // tpRecords用于存放分区中的消息集合
            List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
            // newTpRecords用于存放超时的消息，不带分区信息
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>(tpRecords.size());
            for (ConsumerRecord<String, String> record : tpRecords) {
                if (now - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> System.out.println(tp + ":" + offset.offset()));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
