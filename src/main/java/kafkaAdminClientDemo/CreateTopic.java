package kafkaAdminClientDemo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-08-21 21:50:45
 * @description 使用KafkaAdminClient创建主题
 */
public class CreateTopic {
    final static String BROKER_LIST = "node1:9092";
    final static Integer TIMEOUT_MS = 30000;
    static String topic = "topic-admin";

    public static void main(String[] args) {
        Properties props = initConfig();
        // 创建一个KafkaAdminClient实例，
        // create实际上调用KafkaAdminClient.createInternal方法来实现
        final AdminClient client = AdminClient.create(props);
        // 创建一个3分区，3副本的主题
        final NewTopic newTopic = new NewTopic(topic, 3, (short) 3);
        // 添加配置信息
        final Map<String, String> config = new HashMap<>(2);
        config.put("cleanup.policy", "compact");
        newTopic.configs(config);
        // 创建主题
        final CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static Properties initConfig() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        return props;
    }
}
