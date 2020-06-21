package com.github.guozhen.service.sink;

import com.github.guozhen.config.AppConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class EventTimeBucketAssigner implements BucketAssigner<String, String> {

    @Override
    public String getBucketId(String element, Context context) {
        String partitionValue;
        try {
            partitionValue = getPartitionValue(element);
        } catch (Exception e) {
            partitionValue = "failed_00000000";
        }
        //分区目录名称
        return partitionValue;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    private String getPartitionValue(String element) {
        String[] fields=element.split(AppConstants.FIELD_DELIMITER);

        String database = fields[1];
        String table = fields[2];
        // 取出最后拼接字符串的es字段值，该值为业务时间
        long eventTime = Long.parseLong(fields[3]);
        Date eventDate = new Date(eventTime);
        String dateStr=new SimpleDateFormat(AppConstants.HDFS_PATH_DATE_FORMAT).format(eventDate);
        String partition = String.format("%s/%s/dt=%s",database,table,dateStr);
        log.debug("写入分区：",partition);
        return partition;
    }
}
