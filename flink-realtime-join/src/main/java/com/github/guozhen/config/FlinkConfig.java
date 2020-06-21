package com.github.guozhen.config;

import com.typesafe.config.Config;

public class FlinkConfig extends Configuration{
    private static FlinkConfig ourInstance = new FlinkConfig();

    public static FlinkConfig getInstance() {
        return ourInstance;
    }

    private FlinkConfig() {
    }
    private final Config flinkConf = config.getConfig("flink");
    public final String checkpointUri = flinkConf.getString("checkpoint.uri");
    public final String fsDefaultScheme = flinkConf.getString("fs.default.scheme");
    public final Integer checkpointInterval = flinkConf.getInt("checkpoint.interval");
    // 分区并行度 和kafka topic分区数对应，不能超过kafka topic 分区数
//    public final Integer partitionParallelism = flinkConf.getInt("partition.parallelism");
//    // 执行转换操作的并行度
//    public final Integer executorParallelism = flinkConf.getInt("executor.parallelism");
}
