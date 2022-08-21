package kafkaAdminClientDemo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewPartitions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-08-21 22:55:30
 * @description 将主题分区增加到5
 */
public class CreateTopicPartitions {
    static String topic = "topic-admin";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Properties props = CreateTopic.initConfig();
        final AdminClient client = KafkaAdminClient.create(props);

        final NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>(10);
        newPartitionsMap.put(topic, newPartitions);
        final CreatePartitionsResult result = client.createPartitions(newPartitionsMap);
        result.all().get();
        client.close();
    }
}
