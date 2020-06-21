package com.github.guozhen.service.sink;

import com.github.guozhen.config.AppConstants;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class HdfsFileSink {

    private HdfsFileSink() {
    }

    public static StreamingFileSink<String> getSink(ParameterTool parameterTool){

        // 设置滚动策略：以下条件满足其中之一就会滚动生成新的文件
        RollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.builder()
                //滚动写入新文件的时间，默认60s。根据具体情况调节
                .withRolloverInterval(parameterTool.getInt(AppConstants.FILE_ROLLOVER_INTERVAL) * 1000L)
                //设置每个文件的最大大小 ,默认是128M，这里设置为128M
                .withMaxPartSize(1024 * 1024 * 128L)
                //默认60秒,未写入数据处于不活跃状态超时会滚动新文件
                .withInactivityInterval(parameterTool.getInt(AppConstants.FILE_ROLLOVER_INTERVAL) * 1000L)
                .build();

        // 设置分桶规则
        return StreamingFileSink
                .forRowFormat(new Path(parameterTool.get(AppConstants.HDFS_OUTPUT_PATH)), new SimpleStringEncoder<String>())
                .withBucketAssigner(new EventTimeBucketAssigner())
                .withRollingPolicy(rollingPolicy)
                // 桶检查间隔，这里设置1S
                .withBucketCheckInterval(1000)
                .build();
    }
}
