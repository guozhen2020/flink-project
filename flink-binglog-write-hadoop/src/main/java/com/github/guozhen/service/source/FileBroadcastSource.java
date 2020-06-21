package com.github.guozhen.service.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

/**
 * 测试用
 */
public class FileBroadcastSource extends RichParallelSourceFunction<String> {


    private volatile boolean isRun;
    private volatile int lastUpdateMin = -1;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRun = true;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRun){
            LocalDateTime date = LocalDateTime.now();
            int min = date.getMinute();
            if(min != lastUpdateMin){
                lastUpdateMin = min;
                Set<String> configs = readConfigs();
                if(configs != null && configs.size() > 0){
                    for(String config : configs){
                        ctx.collect(config);
                    }

                }
            }
            Thread.sleep(1000);
        }
    }

    private Set<String> readConfigs(){
        //这里读取配置信息
        Set<String> rule =  new HashSet<>();
        rule.add("false,testDB,t1");
        return rule;
    }


    @Override
    public void cancel() {
        isRun = false;
    }
}
