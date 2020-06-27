package com.github.guozhen;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.github.guozhen.bean.BinglogBean;
import com.github.guozhen.config.AppConstants;
import com.github.guozhen.config.KafkaConstants;
import com.github.guozhen.service.BinglogParseFlatMap;
import com.github.guozhen.service.sink.HbaseSink;
import com.github.guozhen.utils.EnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class Kafka2HBaseUseWatermark {

    private static String[] mysqlCloumns;
    private static String[] hbaseCloumns;

    public static void main(String[] args) throws Exception {

        // 初始化flink运行环境
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (!check(parameterTool)) {
            return;
        }
        final StreamExecutionEnvironment env = EnvUtil.getStreamExecutionEnvironment(parameterTool);
        //设置使用eventtime，默认是使用processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
        SingleOutputStreamOperator<BinglogBean> mapStream =filterStream.flatMap(new BinglogParseFlatMap(mysqlCloumns));
        mapStream.print();

        //抽取timestamp和生成watermark
        DataStream<BinglogBean> waterMarkStream = mapStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<BinglogBean>() {

            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10s
            Long lastEmittedWatermark = Long.MIN_VALUE;

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            /**
             * 定义生成watermark的逻辑，比当前最大时间戳晚10s
             * 默认100ms被调用一次
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                // 允许延迟三秒
                long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
                // 保证水印能依次递增
                if (potentialWM >= lastEmittedWatermark) {
                    lastEmittedWatermark = potentialWM;
                }
                return new Watermark(lastEmittedWatermark);
            }

            //定义如何提取timestamp
            @Override
            public long extractTimestamp(BinglogBean element, long previousElementTimestamp) {
                String table = element.getTable();
                long timestamp =element.getEventTime();
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                //设置多并行度时获取线程id
                long id = Thread.currentThread().getId();
                log.debug("extractTimestamp=======>" + ",currentThreadId:" + id + ",table:" +table + ",eventtime:[" + timestamp + "|" + sdf.format(timestamp) + "]," +
                        "currentMaxTimestamp:[" + currentMaxTimestamp + "|" +
                        sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
                return timestamp;
            }
        });

        // 创建延迟数据的标签
        OutputTag<BinglogBean> lateData = new OutputTag<>("late");

        // 对window内的数据进行排序，保证数据的顺序
        SingleOutputStreamOperator<List<BinglogBean>> windowStream=waterMarkStream.keyBy(t->t.getDatabase()+"|"+t.getTable())
                //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(lateData)
                .apply((WindowFunction<BinglogBean, List<BinglogBean>, String, TimeWindow>) (key, window, input, out) -> {
                    List<BinglogBean> list = new ArrayList<>();
                    for (BinglogBean bean : input) {
                        list.add(bean);
                    }
                    list.sort(Comparator.comparingLong(BinglogBean::getEventTime));

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    String result = key + "," + list.size() + "," + sdf.format(list.get(0)) + "," + sdf.format(list.get(list.size() - 1))
                            + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                    log.debug(result);
                    out.collect(list);
                });

        windowStream.process(new HbaseSink(parameterTool,mysqlCloumns,hbaseCloumns));

        // 迟到数据处理
        DataStream<BinglogBean> lateStream = windowStream.getSideOutput(lateData);
        lateStream.print("迟到的数据:");

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