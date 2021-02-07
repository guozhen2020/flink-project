package com.github.guozhen;

import com.github.guozhen.config.Config;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env,settingsBuilder.build());

        // 创建数据源表：创建hive catalog，创建数据源表sourceTable
        String HIVE_CATALOG = "myhive";
        String DEFAULT_DATABASE = "default";
        String HIVE_CONF_DIR = "hadoop-conf/";
        Catalog catalog = new HiveCatalog(HIVE_CATALOG, DEFAULT_DATABASE, HIVE_CONF_DIR);
        tenv.registerCatalog(HIVE_CATALOG, catalog);
        tenv.useCatalog("myhive");

        tenv.executeSql("DROP TABLE IF EXISTS myhive.test.sourceTable");
        tenv.executeSql("CREATE TABLE myhive.test.sourceTable (\n" +
                " userid int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='5',\n" +
                " 'fields.userid.kind'='random',\n" +
                " 'fields.userid.min'='1',\n" +
                " 'fields.userid.max'='100',\n" +
                "'fields.f_random_str.length'='10'\n" +
                ")");

        //写入阶段：创建一个名为hive类型的 iceberg catalog ，用来从 hive metastore 中加载表
        tenv.executeSql("CREATE CATALOG iceberg WITH (" +
                "  'type'='iceberg'," +
                "  'catalog-type'='hive'," +
                "  'uri'='thrift://namenode1:9083'," +
                "  'warehouse'='hdfs://namenode1/user/hive/warehouse'" +
                ")");

        tenv.useCatalog("iceberg");
        tenv.executeSql("CREATE DATABASE if not exists iceberg_db");
        tenv.useDatabase("iceberg_db");

        tenv.executeSql("DROP TABLE IF EXISTS iceberg.iceberg_db.iceberg_001");
        tenv.executeSql("CREATE TABLE iceberg.iceberg_db.iceberg_001 (\n" +
                " id int,\n" +
                " random_str STRING\n" +
                ") WITH ('connector'='iceberg','write.format.default'='parquet')" );

        // 写入iceberg表
        System.out.println("---> insert into iceberg  table from datagen stream table .... ");
//        tenv.executeSql(
//                "insert into iceberg.iceberg_db.iceberg_001 select userid,f_random_str from myhive.test.sourceTable");

        Table data=tenv.sqlQuery("select userid,f_random_str from myhive.test.sourceTable");
        tenv.createTemporaryView("sourceTable",data);

        sql(tenv,"INSERT INTO %s SELECT userid,f_random_str from sourceTable", "iceberg.iceberg_db.iceberg_001");
    }
    public static final TableSchema FLINK_SCHEMA = TableSchema.builder()
            .field("userid", DataTypes.INT())
            .field("f_random_str", DataTypes.STRING())
            .build();

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

    private List<Object[]> sql(StreamTableEnvironment env,String query, Object... args) {
        TableResult tableResult = env.executeSql(String.format(query, args));

        tableResult.getJobClient().ifPresent(c -> {
            try {
                c.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        List<Object[]> results = Lists.newArrayList();
        try (CloseableIterator<Row> iter = tableResult.collect()) {
            while (iter.hasNext()) {
                Row row = iter.next();
                results.add(IntStream.range(0, row.getArity()).mapToObj(row::getField).toArray(Object[]::new));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return results;
    }
}
