package kafkaTools;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class SimpleKafkaConsumer2 {
    private static KafkaConsumer<String, String> consumer;
    private final static String SOURCE_TOPIC = "test";

    public SimpleKafkaConsumer2(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        //每个消费者分配独立的组号
        props.put("group.id", "test_gz01");
        //如果value合法，则自动提交偏移量
        props.put("enable.auto.commit", "true");
        //设置多久一次更新被消费消息的偏移量
        props.put("auto.commit.interval.ms", "1000");
        //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
        props.put("session.timeout.ms", "30000");
        //自动重置offset
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
    }

    private void consume() throws InterruptedException {
        consumer.subscribe(Arrays.asList(SOURCE_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s",record.offset(), record.key(), record.value());
                System.out.println();
            }
            Thread.sleep(10);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new SimpleKafkaConsumer2().consume();
    }
}