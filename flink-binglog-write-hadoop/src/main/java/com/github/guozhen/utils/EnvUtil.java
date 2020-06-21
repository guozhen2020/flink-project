package com.github.guozhen.utils;

import com.github.guozhen.config.AppConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvUtil {

    public static StreamExecutionEnvironment getStreamExecutionEnvironment(ParameterTool parameterTool){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(parameterTool.getInt(AppConstants.FLINK_PARTITION_PARALLELISM));
        // checkpoint
        env.enableCheckpointing(parameterTool.getInt(AppConstants.FLINK_CHECKPOINT_INTERVAL));
//        env.setStateBackend((StateBackend) new FsStateBackend("file:///D://temp/flink-ckdir"));
        env.setStateBackend((StateBackend) new FsStateBackend(parameterTool.get(AppConstants.FLINK_CHECKPOINT_URI)));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 设置 checkpoint 最小间隔 500 ms
        config.setMinPauseBetweenCheckpoints(parameterTool.getInt(AppConstants.FLINK_CHECKPOINT_MIN_INTERVAL));
        // 设置 exactly-once 模式
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        System.setProperty("user.name", parameterTool.get(AppConstants.HADOOP_USER_NAME));
//        System.setProperty("HADOOP_USER_NAME", parameterTool.get(AppConstants.HADOOP_USER_NAME));

        return env;
    }
}
