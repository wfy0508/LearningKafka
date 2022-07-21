package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-18 16:54:03
 * @description 创建一个Kafka生产者，用于发送超时消息和正常消息
 */
public class ProducerExpireRecords {
    /**
     * 自定义超时时间
     */
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. 创建生产者配置对象
        Properties props = new Properties();
        // 2. 配置bootstrap.servers属性
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        // 3. 配置key, value序列化属性
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 分区器
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner.DemoPartitioner.class.getName());
        // 拦截器(多个拦截器用逗号隔开)
        //props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptor.ProducerInterceptorPrefix.class.getName() + "," + interceptor.ProducerInterceptorPlus.class.getName());

        // 4. 创建生产者对象
        KafkaProducer<String, String> producerTest = new KafkaProducer<>(props);
        // 5. 创建第一条超时消息
        ProducerRecord<String, String> record1 = new ProducerRecord<>(
                "test",
                0,
                System.currentTimeMillis() - EXPIRE_INTERVAL,
                null,
                "first_expire_record");
        producerTest.send(record1).get();

        // 创建一条正常消息
        ProducerRecord<String, String> record2 = new ProducerRecord<>(
                "test",
                0,
                System.currentTimeMillis(),
                null,
                "normal_data");
        producerTest.send(record2).get();

        // 创建第二条超时消息
        ProducerRecord<String, String> record3 = new ProducerRecord<>(
                "test",
                0,
                System.currentTimeMillis() - EXPIRE_INTERVAL,
                null,
                "second_expire_record");
        producerTest.send(record3).get();

        producerTest.close();

    }
}
