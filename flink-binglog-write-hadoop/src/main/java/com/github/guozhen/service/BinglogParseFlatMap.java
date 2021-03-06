package com.github.guozhen.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.github.guozhen.bean.BinglogBean;
import com.github.guozhen.config.AppConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class BinglogParseFlatMap implements FlatMapFunction<String, BinglogBean>{

    private String[] cloumns;
    public BinglogParseFlatMap(String[] cloumns){
        this.cloumns=cloumns;
    }

    /**
     * 解析binglog
     * @param value 输入的一条binglog记录
     * @param out 输出解析后的数据
     */
    @Override
    public void flatMap(String value, Collector<BinglogBean> out){

        String fieldDelimiter = AppConstants.FIELD_DELIMITER;
        StringBuilder fieldsBuilder = new StringBuilder();
        // 解析JSON数据
        JSONObject record = JSON.parseObject(value, Feature.OrderedField);
        // 获取mysql记录，单次操作只会有一条记录，事务操作会有多条记录
        JSONArray data = record.getJSONArray("data");
        for (int i = 0; i < data.size(); i++) {
            JSONObject obj = data.getJSONObject(i);
            if (obj != null) {
                BinglogBean bean = new BinglogBean();
                // 序号id
                bean.setId(record.getLong("id"));
                // 库名
                bean.setDatabase(record.getString("database"));
                // 表名
                bean.setTable(record.getString("table"));
                //业务时间戳
                bean.setEventTime(record.getLong("es"));
                // 日志时间戳
                bean.setLogTime(record.getLong("ts"));
                // 操作类型
                bean.setOptType(record.getString("type"));
                // 表字段数据
                for (String cloumn : cloumns) {
                    fieldsBuilder.append(obj.get(cloumn));
                    fieldsBuilder.append(fieldDelimiter);
                }
                String binglogData = fieldsBuilder.toString();
                binglogData=binglogData.substring(0,binglogData.length() - 1);
                bean.setData(binglogData);

                out.collect(bean);
                //清空缓存
                fieldsBuilder.delete(0, fieldsBuilder.length());
            }
        }
    }
}
