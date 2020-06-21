package com.github.guozhen;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.github.guozhen.config.*;
import com.github.guozhen.service.BinglogParseFlatMap;
import com.github.guozhen.service.BroadcastProcess;
import com.github.guozhen.service.sink.HdfsFileSink;
import com.github.guozhen.service.source.MySqlSource;
import com.github.guozhen.utils.EnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

@Slf4j
public class Kafka2Hdfs {

    private static String[] mysqlCloumns;

    public static void main(String[] args) throws Exception {

        // 初始化flink运行环境
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (!check(parameterTool)) {
            return;
        }
        final StreamExecutionEnvironment env = EnvUtil.getStreamExecutionEnvironment(parameterTool);


        //mysql binglog — 业务流
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>(
                parameterTool.get(KafkaConstants.TOPICS), new SimpleStringSchema(), KafkaConstants.getKafkaProperties(parameterTool))
        );
        stream.print();

        // 广播状态描述
        final MapStateDescriptor<String, List<String>> broadcastStateDesc = new MapStateDescriptor<>(
                "config-keywords",
                BasicTypeInfo.STRING_TYPE_INFO,
                new ListTypeInfo<>(String.class));

        // 规则流 -- 广播流
        BroadcastStream<List<String>> broadcastStream = env.addSource(new MySqlSource())
                .setParallelism(1)
                // 将基础流转为广播流的时候需要指定广播流的描述信息
                .broadcast(broadcastStateDesc);

        // 业务流和广播流连接处理并将拦截，过滤掉DDL操作
        DataStream<String> connectedStream = stream.connect(broadcastStream).process(new BroadcastProcess(broadcastStateDesc));
        connectedStream.print();

        SingleOutputStreamOperator<String> stringStream = connectedStream.flatMap(new BinglogParseFlatMap(mysqlCloumns));
        stringStream.print();
        stringStream.addSink(HdfsFileSink.getSink(parameterTool));

        env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }

    private static boolean check(ParameterTool parameterTool){
        boolean flag = true;

        mysqlCloumns = parameterTool.get(AppConstants.MYSQL_TABLE_COLUMNS).split(AppConstants.FIELD_DELIMITER);
        if(mysqlCloumns.length == 0){
            log.error(String.format("参数错误（%s）：请输入要同步的mysql表的字段，多个值用逗号隔开！",AppConstants.MYSQL_TABLE_COLUMNS));
            flag =false;
        }

        return flag;
    }
}