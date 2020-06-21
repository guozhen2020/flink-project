package com.github.guozhen.config;

import com.typesafe.config.Config;

import java.util.Properties;

public class KafkaConfig extends Configuration {
    private static KafkaConfig ourInstance = new KafkaConfig();

    public static KafkaConfig getInstance() {
        return ourInstance;
    }

    private KafkaConfig() {
        props.setProperty("bootstrap.servers", this.bootstrapServers);
        // only required for Kafka 0.8
        // props.setProperty("zookeeper.connect", "192.168.0.10:2181");
        props.setProperty("auto.offset.reset", this.autoOffsetReset);
        props.setProperty("enable.auto.commit", this.enableAutoCommit);
        props.setProperty("auto.commit.interval.ms", this.autoCommitIntervalMs);
        props.setProperty("group.id", this.groupId);
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
    private final Config kafkaConf = config.getConfig("kafka");
    public final Properties props = new Properties();
    public final String bootstrapServers = kafkaConf.getString("bootstrap.servers");
    public final String sourceTopics = kafkaConf.getString("source.topics");
    public final String groupId = kafkaConf.getString("group.id");//+ System.currentTimeMillis() TODO 上线的时候这个地方要删除掉
    public final String autoOffsetReset = kafkaConf.getString("auto.offset.reset");
    public final String enableAutoCommit = kafkaConf.getString("enable.auto.commit");
    public final String autoCommitIntervalMs = kafkaConf.getString("auto.commit.interval.ms");
    public final String maxRequestSize = kafkaConf.getString("max.request.size");



}
