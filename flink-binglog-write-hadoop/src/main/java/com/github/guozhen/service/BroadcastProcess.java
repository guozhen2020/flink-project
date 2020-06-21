package com.github.guozhen.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.HeapBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

@Slf4j
public class BroadcastProcess extends BroadcastProcessFunction<String, List<String>, String> {

    private static final String KEY ="1";
    // 广播状态描述
    private  MapStateDescriptor<String, List<String>> broadcastStateDesc;

    public BroadcastProcess(MapStateDescriptor<String, List<String>> broadcastStateDesc){
        this.broadcastStateDesc=broadcastStateDesc;
    }

    // 每当 主体基本流新增一条记录，该方法就会执行一次
    @Override
    public void processElement(String dataRow, ReadOnlyContext broadcastCtx, Collector<String> collector) throws Exception {

        // 从 广播状态中根据key获取数据(规则数据)
        HeapBroadcastState<String,List<String>> rule = (HeapBroadcastState)broadcastCtx.getBroadcastState(broadcastStateDesc);
        List<String> ruleSet = rule.get(KEY);
        for (String ruleRow : ruleSet) {
            log.info("规则库:" + ruleRow);
        }

        // 处理每一个元素，看state是否有匹配的，有的话，下发到下一个节点
        final String fieldDelimiter=",";
        JSONObject record = JSON.parseObject(dataRow, Feature.OrderedField);
        String key = record.getString("isDdl")+fieldDelimiter+record.getString("database")+fieldDelimiter+record.getString("table");
        if (ruleSet.contains(key))
        {
            collector.collect(dataRow);
            log.info("key {"+key+"} 命中规则！");
        }
        else{
            log.info("key {"+key+"} 未命中规则！");
        }
    }

    // 每当 广播流新增一条记录，该方法就会执行一次
    @Override
    public void processBroadcastElement(List<String> ruleRow, Context ctx, Collector<String> collector) throws Exception {
        log.info("收到广播:" + ruleRow);
        BroadcastState<String,List<String>> state =  ctx.getBroadcastState(broadcastStateDesc);
        state.clear();
        state.put(KEY,ruleRow);
    }
}
