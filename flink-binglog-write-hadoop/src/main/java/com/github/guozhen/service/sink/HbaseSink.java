package com.github.guozhen.service.sink;

import com.github.guozhen.bean.BinglogBean;
import com.github.guozhen.config.AppConstants;
import com.github.guozhen.config.HBaseConstants;
import com.github.guozhen.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HbaseSink extends ProcessFunction<List<BinglogBean>, String> {
    private static final long serialVersionUID = 1L;

    private List<Put> puts = new ArrayList<>(AppConstants.HBASE_DATA_SUBMIT_MAX_SIZE);
    private ParameterTool parameterTool;
    private Map<String,Integer>  hbaseColumnIndexMap = new HashMap<>(32);
    private Map<String,Integer>  mysqlColumnIndexMap = new HashMap<>(32);

    private HbaseSink(){

    }

    public HbaseSink(ParameterTool parameterTool, String[] mysqlColumns, String[] hbaseColumns){
        this.parameterTool=parameterTool;
        setHbaseColumnIndexMap(mysqlColumns,hbaseColumns);
    }

    private transient Connection connection = null;


    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {

        try {
            // 加载HBase的配置
            Configuration configuration = HBaseConfiguration.create();

            // 读取配置文件
            configuration.set("hbase.zookeeper.quorum", parameterTool.get(HBaseConstants.HBASE_ZOOKEEPER_QUORUM));
            configuration.set("hbase.zookeeper.property.clientPort",  parameterTool.get(HBaseConstants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
            configuration.setInt("hbase.rpc.timeout", HBaseConstants.HBASE_RPC_TIMEOUT);
            configuration.setInt("hbase.client.operation.timeout", HBaseConstants.HBASE_CLIENT_OPERATION_TIMEOUT);
            configuration.setInt("hbase.client.scanner.timeout.period", HBaseConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
            //hbase在zookeeper上的目录不是默认路径时，需要配置
             configuration.set("zookeeper.znode.parent",parameterTool.get(HBaseConstants.HBASE_ZOOKEEPER_ZNODE_PARENT));
            configuration.set("hbase.master",parameterTool.get(HBaseConstants.HBASE_MASTER));
            connection = ConnectionFactory.createConnection(configuration);

            log.info("[HbaseSink] : open hbase connection finished");
        } catch (Exception e) {
            log.error(e.getMessage());
            throw e;
        }
    }

    /**
     * 构建mysql列与hbase列的映射关系
     * @param mysqlColumns mysql 列 的集合
     * @param hbaseColumns hbase  列簇:列 的集合
     */
    private void setHbaseColumnIndexMap(String[] mysqlColumns,String[] hbaseColumns){

        for(int i=0;i<mysqlColumns.length;i++){
            mysqlColumnIndexMap.put(mysqlColumns[i],i);
        }

        for (String cfCloumn : hbaseColumns) {

            // cf:name
            Integer cloumnIndex = mysqlColumnIndexMap.get(cfCloumn.split(":")[1]);
            hbaseColumnIndexMap.put(cfCloumn, cloumnIndex);
        }
    }

    @Override
    public void close() throws Exception {
        log.debug("hbase close...");
        if (null != connection) {
            connection.close();
        }
    }

    @Override
    public void processElement(List<BinglogBean> value, Context ctx, Collector<String> out) throws Exception {
        Table table = null;
        try {
            log.debug("本次同步数量:"+value.size());
            for (BinglogBean bean: value) {
                // 业务时间戳
                long eventTime =bean.getEventTime();
                String[] cloumns = bean.getData().split(AppConstants.FIELD_DELIMITER);
                String rowKey = setHbaseRowkey(cloumns);
                //创建一个put请求，用于添加数据或者更新数据
                Put put = new Put(rowKey.getBytes());

                // 添加值：f1->列族, order->属性名 如age， 第三个->属性值 如25,通过设置时间戳来保证延迟数据的正确性
                for (Map.Entry<String,Integer> entry: hbaseColumnIndexMap.entrySet()) {
                    String[] cfColumn = entry.getKey().split(":");
                    String cf = cfColumn[0];
                    String column = cfColumn[1];
                    put.addColumn(cf.getBytes(), column.getBytes(),eventTime, cloumns[entry.getValue()].getBytes());
                }
                puts.add(put);
            }

            if(puts.size()>0){
                //获取一个Hbase表
                table = connection.getTable(TableName.valueOf(parameterTool.get(AppConstants.HBASE_MYSQL_MAPPING_DB_TABLE)));
                //批次提交
                table.put(puts);
                puts.clear();
                table.close();
                log.debug("插入成功");
            }
        } catch (Exception e) {
            if (null != table) {
                table.close();
            }
            log.error(e.getMessage());
            throw e;
        }
    }

    private String setHbaseRowkey(String[] cloumns){
        String rowkeyFormat = parameterTool.get(AppConstants.HBASE_MYSQL_MAPPING_ROWKEY);
        List<String> rkParamters = RegexUtils.matchBrace(rowkeyFormat);

        for (String macher:rkParamters) {
            // macher={xxx},获取rowkey中的列所在的value中的下标
            Integer columnIndex = mysqlColumnIndexMap.get(macher.replace("{","").replace("}",""));

            //下标对应的列的值,替换rowkey中的变量
            rowkeyFormat=rowkeyFormat.replace("$"+macher,cloumns[columnIndex]);
        }
        return rowkeyFormat;
    }
}