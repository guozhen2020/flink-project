package com.github.guozhen.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.github.guozhen.bean.BinglogBean;
import com.github.guozhen.config.AppConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class BinglogParseFlatMapToString implements FlatMapFunction<String, String>{

    private String[] cloumns;
    public BinglogParseFlatMapToString(String[] cloumns){
        this.cloumns=cloumns;
    }

    /**
     * 解析binglog
     * @param value 输入的一条binglog记录
     * @param out 输出解析后的数据
     * @throws Exception
     */
    @Override
    public void flatMap(String value, Collector<String> out){

        String fieldDelimiter = AppConstants.FIELD_DELIMITER;
        StringBuilder fieldsBuilder = new StringBuilder();
        // 解析JSON数据
        JSONObject record = JSON.parseObject(value, Feature.OrderedField);
        // 获取mysql记录，单次操作只会有一条记录，事务操作会有多条记录
        JSONArray data = record.getJSONArray("data");
        for (int i = 0; i < data.size(); i++) {
            JSONObject obj = data.getJSONObject(i);
            if (obj != null) {
                fieldsBuilder.append(record.getLong("id")); // 序号id
                fieldsBuilder.append(fieldDelimiter);
                fieldsBuilder.append(record.getString("database")); // 库名
                fieldsBuilder.append(fieldDelimiter);
                fieldsBuilder.append(record.getString("table")); // 表名
                fieldsBuilder.append(fieldDelimiter);
                fieldsBuilder.append(record.getLong("es")); //业务时间戳
                fieldsBuilder.append(fieldDelimiter);
                fieldsBuilder.append(record.getLong("ts")); // 日志时间戳
                fieldsBuilder.append(fieldDelimiter);
                fieldsBuilder.append(record.getString("type")); // 操作类型
                for (String cloumn : cloumns) {
                    fieldsBuilder.append(fieldDelimiter);
                    fieldsBuilder.append(obj.get(cloumn)); // 表字段数据
                }

//                for (Map.Entry<String, Object> entry : obj.entrySet()) {
//                    fieldsBuilder.append(fieldDelimiter);
//                    fieldsBuilder.append(entry.getValue()); // 表字段数据
//                }

                out.collect(fieldsBuilder.toString());
                //清空缓存
                fieldsBuilder.delete(0, fieldsBuilder.length());
            }
        }
    }
}
