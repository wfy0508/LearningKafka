import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author summer
 * @project_name IntelliJ IDEA
 * @create_time 2022-06-23 11:20:02
 * @description
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 给原始value值，加上前缀prefix1-
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<String, String>(
                record.topic(),
                record.partition(),
                record.timestamp(),
                record.key(),
                modifiedValue,
                record.headers()
        );
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendSuccess + sendFailure);
        System.out.println("[Info] 发送成功率：" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
