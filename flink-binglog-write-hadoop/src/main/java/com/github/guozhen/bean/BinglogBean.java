package com.github.guozhen.bean;

import java.io.Serializable;

public class BinglogBean implements Serializable {
    //序号id
    private long id;
    // 库名
    private String database;
    // 表名
    private String table;
    //业务时间戳
    private long eventTime;
    // 日志时间戳
    private long logTime;
    // mysql操作类型
    private String optType;
    //mysql操作记录
    private String data;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public long getLogTime() {
        return logTime;
    }

    public void setLogTime(long logTime) {
        this.logTime = logTime;
    }

    public String getOptType() {
        return optType;
    }

    public void setOptType(String optType) {
        this.optType = optType;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
