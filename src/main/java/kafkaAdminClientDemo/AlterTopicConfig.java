package kafkaAdminClientDemo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-08-21 22:46:35
 * @description 为主题增加配置信息cleanup.policy=compact
 */
public class AlterTopicConfig {
    static String topic = "topic-admin";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Properties props = CreateTopic.initConfig();
        // 创建一个KafkaAdminClient实例
        final AdminClient client = AdminClient.create(props);
        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        final ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
        final Config config = new Config(Collections.singleton(entry));
        final Map<ConfigResource, Config> configs = new HashMap<>(10);
        configs.put(resource, config);
        final AlterConfigsResult result = client.alterConfigs(configs);
        result.all().get();
        client.close();
    }
}
