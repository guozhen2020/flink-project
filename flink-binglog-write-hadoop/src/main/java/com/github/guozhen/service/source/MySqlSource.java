package com.github.guozhen.service.source;

import com.github.guozhen.config.MysqlConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class MySqlSource extends RichParallelSourceFunction<List<String>> {

    private volatile boolean isRun;
    private volatile int lastUpdateMin = -1;

    private transient PreparedStatement ps;
    private Connection connection;

    // 用来建立连接
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String sql = "select id,isddl,db_name,table_name from gz_test_binglog_rule";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
            isRun = true;
            log.info("MySqlSource open");
        } else {
            log.error("MySqlSource open failed!");
        }
    }

    @Override
    public void close() throws Exception {
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
        log.info("MySqlSource close");
    }

    private Connection getConnection() {
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Connection con = null;
        try {
            Class.forName(MysqlConstants.JDBC_DRIVER);
            String url = parameterTool.get(MysqlConstants.URL);
            String username = parameterTool.get(MysqlConstants.USERNAME);
            String password = parameterTool.get(MysqlConstants.PASSWORD);
            con = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }

    /**
     * 此处是代码的关键，要从mysql表中，把数据读取出来，转成Map进行数据的封装
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<List<String>> sourceContext) throws Exception {
        Set<String> rule = new HashSet<>();
        while (isRun) {
            LocalDateTime date = LocalDateTime.now();
            int min = date.getMinute();
            if (min != lastUpdateMin) {
                lastUpdateMin = min;
                ResultSet resultSet = ps.executeQuery();
                rule.clear();
                String fieldDelimiter = ",";
                while (resultSet.next()) {
                    String isddl = resultSet.getString("isddl");
                    String dbName = resultSet.getString("db_name");
                    String tableName = resultSet.getString("table_name");
                    String key = isddl + fieldDelimiter + dbName + fieldDelimiter + tableName;
                    rule.add(key);
                }

                sourceContext.collect(new ArrayList<>(rule));
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}