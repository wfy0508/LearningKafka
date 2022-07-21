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
 * @description 消费者拦截器，实现超时消息拦截处理
 */

public class ConsumerInterceptorTest implements ConsumerInterceptor<String, String> {
    /**
     * 自定义超时时间
     */
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        // 获取当前时间
        long now = System.currentTimeMillis();
        // 定义一个Map用于存放消息的分区信息，消息的key和value
        Map<TopicPartition, List<ConsumerRecord<String, String>>> receivedMessage = new HashMap<>(100);
        // 获取消息的分区信息
        for (TopicPartition tp : records.partitions()) {
            // 获取分区内的消息
            List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
            // 定义一个列表，用于存放超时的消息
            List<ConsumerRecord<String, String>> expireMassage = new ArrayList<>();
            // 获取消息的时间戳
            for (ConsumerRecord<String, String> record : tpRecords) {
                // 判断消息是否超时
                if (now - record.timestamp() > EXPIRE_INTERVAL) {
                    // 超时的消息存入expireMessage中
                    expireMassage.add(record);
                }
            }
            // 如果expireMessage非空，就将其放入receivedMessage中
            if (!expireMassage.isEmpty()) {
                receivedMessage.put(tp, expireMassage);
            }
        }
        // 返回超时消息集合
        return new ConsumerRecords<>(receivedMessage);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> System.out.println("partition: " + tp + ", offset: " + offset));

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
