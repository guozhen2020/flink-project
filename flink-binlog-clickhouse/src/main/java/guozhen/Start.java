package guozhen;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;

import guozhen.config.FlinkConfig;
import guozhen.config.KafkaConfig;
import guozhen.service.source.MysqlAsyncLookupTableSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class Start {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final FlinkConfig flinkConfig= FlinkConfig.getInstance();
    private static final KafkaConfig kafkaConfig=KafkaConfig.getInstance();


    public static void main(String[] args) throws Exception {

        init();
        final EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //source mysql binglog 流 — 业务流
        DataStream<String> stream = env.addSource(getSource());
        //stream.print();

        //解析binglog
        TypeInformation[] types = new TypeInformation[]{Types.LONG,Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.STRING, Types.STRING, Types.STRING, Types.STRING};
        String[] fields = new String[]{"id", "database", "table", "es", "ts", "type", "orderId", "userId", "carId"};
        RowTypeInfo typeInformation = new RowTypeInfo(types, fields);
        DataStream<Row> recordStream =stream.map((MapFunction<String, Row>) record->parseJson(record,fields,types))
                .returns(typeInformation);
        tableEnv.createTemporaryView("bg_order", recordStream, String.join(",", typeInformation.getFieldNames()) + ",proctime.proctime");
        recordStream.print();

        // 维表
        MysqlAsyncLookupTableSource tableSource = MysqlAsyncLookupTableSource.Builder.newBuilder()
                .withTableNames("order_snapshot_202006")
                .withFieldNames(new String[]{"plate_num", "take_parking_name", "energy", "order_id"})
                .withFieldTypes(new TypeInformation[]{Types.STRING, Types.STRING, Types.INT, Types.STRING})
                .withConnectionField(new String[]{"order_id"})
                .build();
        tableEnv.registerTableSource("order_snapshot", tableSource);

        // join操作
        String sql = "select t1.orderId,t1.type,t2.plate_num,t2.take_parking_name,t2.energy  " +
                " from bg_order as t1" +
                " join order_snapshot FOR SYSTEM_TIME AS OF t1.proctime t2" +
                " on t1.orderId = t2.order_id ";

        Table table = tableEnv.sqlQuery(sql);
        DataStream<Row> result = tableEnv.toAppendStream(table, Row.class);
        result.print();


        //recordStream.print();
//        recordStream.addSink(getSink());
        env.execute(Thread.currentThread().getStackTrace()[1].getClassName());
    }

    /***
     * 解析binglog
     * @param value 输入的一条binglog记录
     * @return 解析后的数据结构
     */
    private static Row parseJson(String value,String[] fields,TypeInformation[] types){
        // 解析JSON数据
        JSONObject record = JSON.parseObject(value, Feature.OrderedField);
        // 获取data数组
        JSONArray data = record.getJSONArray("data");
        Row row = new Row(fields.length);
        if(data.size()==1){
            // 获取到JSON数组的第i个元素
            JSONObject obj = data.getJSONObject(0);
            if (obj != null) {
                for(int i=0;i<fields.length;i++){
                    if (types[i].equals(Types.STRING)) {
                        String item = record.getString(fields[i]);
                        if(item==null){
                            item = obj.getString(fields[i]);
                        }
                        row.setField(i,item);
                    }
                    if (types[i].equals(Types.LONG)) {
                        Long item = record.getLong(fields[i]);
                        if(item==null){
                            item = obj.getLong(fields[i]);
                        }
                        row.setField(i, item);
                    }
                }
            }
        }
        return row;
    }

    // kafka source
    private static FlinkKafkaConsumer<String> getSource(){
        return new FlinkKafkaConsumer<>(kafkaConfig.sourceTopics,new SimpleStringSchema(), kafkaConfig.props);
    }

    // 初始化配置数据
    private static void init() {
        env.setParallelism(1);
        // checkpoint
        env.enableCheckpointing(flinkConfig.checkpointInterval);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///D://temp/flink-ckdir"));
//        env.setStateBackend((StateBackend) new FsStateBackend(flinkConfig.checkpointUri));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 设置 checkpoint 最小间隔 500 ms
        config.setMinPauseBetweenCheckpoints(flinkConfig.checkpointInterval);
        // 设置 exactly-once 模式
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //第一种方式：路径写自己代码上的路径,将hadoop配置文件放到resources目录下
//        properties.setProperty("fs.hdfs.hadoopconf", "...\\src\\main\\resources");
        //第二种方式：填写一个schema参数即可
//        props.setProperty("fs.default-scheme", flinkConfig.fsDefaultScheme);
    }
}
