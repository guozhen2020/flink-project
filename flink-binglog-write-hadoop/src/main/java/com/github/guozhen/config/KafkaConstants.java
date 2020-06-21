package com.github.guozhen.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

public class KafkaConstants {

    public static final String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String TOPICS = "kafka.topics";
    public static final String GROUP_ID = "kafka.group.id";
    public static final String AUTO_OFFSET_RESET = "kafka.auto.offset.reset";
    public static final String ENABLE_AUTO_COMMIT = "kafka.enable.auto.commit";
    public static final String AUTO_COMMIT_INTERVAL_MS = "kafka.auto.commit.interval.ms";

    public static Properties getKafkaProperties(ParameterTool parameterTool) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", parameterTool.get(KafkaConstants.BOOTSTRAP_SERVERS));
        // only required for Kafka 0.8
        // props.setProperty("zookeeper.connect", "192.168.0.10:2181");
        props.setProperty("auto.offset.reset", parameterTool.get(KafkaConstants.AUTO_OFFSET_RESET));
        props.setProperty("enable.auto.commit", parameterTool.get(KafkaConstants.ENABLE_AUTO_COMMIT));
        props.setProperty("auto.commit.interval.ms", parameterTool.get(KafkaConstants.AUTO_COMMIT_INTERVAL_MS));
        props.setProperty("group.id", parameterTool.get(KafkaConstants.GROUP_ID));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //第一种方式：路径写自己代码上的路径,将hadoop配置文件放到resources目录下
//        properties.setProperty("fs.hdfs.hadoopconf", "...\\src\\main\\resources");
        //第二种方式：填写一个schema参数即可,本机测试需注释掉
//        props.setProperty("fs.default-scheme", parameterTool.get(AppConstants.FLINK_FS_DEFAULT_SCHEME));

        return props;
    }
}
