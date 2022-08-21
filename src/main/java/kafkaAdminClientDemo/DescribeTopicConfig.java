package kafkaAdminClientDemo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-08-21 22:34:27
 * @description 使用describeConfigs获取主题的所有配置信息
 */
public class DescribeTopicConfig {
    static String topic = "topic-admin";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Properties props = CreateTopic.initConfig();
        // 创建一个KafkaAdminClient实例
        final AdminClient client = AdminClient.create(props);
        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        final DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        final Config config = result.all().get().get(resource);
        System.out.println(config);
        client.close();
    }
}
