package com.github.guozhen;

import com.github.guozhen.config.Config;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

import static com.github.guozhen.config.Parameters.*;


/***
 * flink cdc to iceberg
 * 参考：https://github.com/zhangjun0x01/bigdata-examples/blob/master/iceberg/src/main/java/com/Flink2Iceberg.java
 * 参考：https://blog.csdn.net/u010834071/article/details/112850376
 * 参考：https://zhengqiang.blog.csdn.net/article/details/112507474
 */
public class AppExecutor {
    private Config config;

    AppExecutor(Config config) {
        this.config = config;
    }

    public void run() throws Exception {

        // Environment setup
        StreamExecutionEnvironment env = configureStreamExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //创建一个名为hive_catalog的 iceberg catalog ，用来从 hive metastore 中加载表
        tenv.executeSql("CREATE CATALOG iceberg WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive'," +
                "  'uri'='thrift://namenode1:9083'," +
                "  'warehouse'='hdfs://namenode1/user/hive/warehouse'" +
                ")");

        tenv.useCatalog("iceberg");
        tenv.executeSql("CREATE DATABASE iceberg_db");
        tenv.useDatabase("iceberg_db");

        tenv.executeSql("CREATE TABLE iceberg.iceberg_db.sourceTable (\n" +
                " userid int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.userid.kind'='random',\n" +
                " 'fields.userid.min'='1',\n" +
                " 'fields.userid.max'='100',\n" +
                "'fields.f_random_str.length'='10'\n" +
                ")");

        tenv.executeSql("CREATE TABLE iceberg.iceberg_db.iceberg_001 (\n" +
                " id int,\n" +
                " random_str STRING\n" +
                ") " );

        tenv.executeSql(
                "insert into iceberg.iceberg_db.iceberg_001 select * from iceberg.iceberg_db.sourceTable");
    }

    private StreamExecutionEnvironment configureStreamExecutionEnvironment() {

        boolean isLocal = config.get(LOCAL_EXECUTION);
        boolean enableCheckpoints = config.get(ENABLE_CHECKPOINTS);
        int checkpointsInterval = config.get(CHECKPOINT_INTERVAL);
        int minPauseBtwnCheckpoints = config.get(CHECKPOINT_INTERVAL);

        Configuration flinkConfig = new Configuration();
        flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        StreamExecutionEnvironment env =
                isLocal
                        ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
                        : StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        if (enableCheckpoints) {
            env.enableCheckpointing(checkpointsInterval);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBtwnCheckpoints);
        }

        // 重启策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));

        return env;
    }

}
