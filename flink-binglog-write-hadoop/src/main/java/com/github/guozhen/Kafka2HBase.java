package com.github.guozhen;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.github.guozhen.bean.BinglogBean;
import com.github.guozhen.config.AppConstants;
import com.github.guozhen.config.KafkaConstants;
import com.github.guozhen.service.BinglogParseFlatMap;
import com.github.guozhen.service.sink.HbaseProcessSink;
import com.github.guozhen.utils.EnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

@Slf4j
public class Kafka2HBase {

    private static String[] mysqlCloumns;
    private static String[] hbaseCloumns;

    public static void main(String[] args) throws Exception {

        // 初始化flink运行环境
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (!check(parameterTool)) {
            return;
        }
        final StreamExecutionEnvironment env = EnvUtil.getStreamExecutionEnvironment(parameterTool);

        //mysql binglog 流
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>(
                parameterTool.get(KafkaConstants.TOPICS), new SimpleStringSchema(), KafkaConstants.getKafkaProperties(parameterTool))
        );
        stream.print();

        // 过滤掉DDL操作
        DataStream<String> filterStream = stream.filter(dataRow->{
            JSONObject record = JSON.parseObject(dataRow, Feature.OrderedField);
            String[] dbTable = parameterTool.get(AppConstants.MYSQL_DATABASE_TABLE).split(AppConstants.FIELD_DELIMITER);
            return "false".equals(record.getString("isDdl")) && record.getString("database").equals(dbTable[0]) && record.getString("table").equals(dbTable[1]);
        });

        //解析binglog json
        SingleOutputStreamOperator<BinglogBean> stringStream =filterStream.flatMap(new BinglogParseFlatMap(mysqlCloumns));
        stringStream.print();

        stringStream.process(new HbaseProcessSink(parameterTool,mysqlCloumns,hbaseCloumns));
        env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }

    private static boolean check(ParameterTool parameterTool){
        boolean flag = true;

        mysqlCloumns = parameterTool.get(AppConstants.MYSQL_TABLE_COLUMNS).split(AppConstants.FIELD_DELIMITER);
        if(mysqlCloumns.length == 0){
            log.error(String.format("参数错误（%s）：请输入要同步的mysql表的字段，多个值用逗号隔开！",AppConstants.MYSQL_TABLE_COLUMNS));
            flag =false;
        }


        hbaseCloumns = parameterTool.get(AppConstants.HBASE_MYSQL_MAPPING_COLUMNS).split(AppConstants.FIELD_DELIMITER);
        if(hbaseCloumns.length==0 ){
            log.error(String.format("参数错误（%s）：请输入Hbase的列名，多个值用逗号隔开！",AppConstants.HBASE_MYSQL_MAPPING_COLUMNS));
            flag =false;
        }
        return flag;
    }
}