package kafkaTools;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class SimpleKafkaConsumer {
    private static KafkaConsumer<String, String> consumer;
    //private final static String SOURCE_TOPIC = "osp-create-data-origin_test";
    private final static String SOURCE_TOPIC = "osp-data-origin-sy";

    private static KafkaProducer<String, String> producer;
    private final static String TARGET_TOPIC = "osp-data-origin-sy";

    public SimpleKafkaConsumer(){
        Properties props = new Properties();
        //props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        props.put("bootstrap.servers", "58.87.69.169:9092,58.87.100.232:9092,58.87.68.229:9092");
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

    public void ProducerSetup(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //设置分区类,根据key进行数据分区
        producer = new KafkaProducer<String, String>(props);
    }

    public void consume() throws InterruptedException {
        consumer.subscribe(Arrays.asList(SOURCE_TOPIC));
        this.ProducerSetup();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s",record.offset(), record.key(), record.value());
                System.out.println();
                producer.send(new ProducerRecord<String, String>(TARGET_TOPIC,record.key(),record.value()));
            }
            Thread.sleep(10);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new SimpleKafkaConsumer().consume();
    }
}