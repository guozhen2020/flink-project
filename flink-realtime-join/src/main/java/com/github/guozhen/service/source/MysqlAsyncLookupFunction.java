package com.github.guozhen.service.source;

import com.github.guozhen.config.MySqlConfig;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


@Slf4j
public class MysqlAsyncLookupFunction extends AsyncTableFunction<Row> {

    private final MySqlConfig mysqlConfig = MySqlConfig.getInstance();

    private transient JDBCClient jdbcClient = null;
    private String tableName;
    private final String[] fieldNames;
    private final String[] connectionField;
    private final TypeInformation[] fieldTypes;

    private MysqlAsyncLookupFunction(String tableName,String[] fieldNames, String[] connectionField, TypeInformation[] fieldTypes) {
        this.tableName=tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.connectionField = connectionField;
    }

    /**
     *   根据传递的keys异步查询维表数据
     * @param resultFuture 当一部访问完成时，调用弃方法进行处理
     * @param keys   源表某些字段的值，通常用来做数据筛选时使用
     */
    public void eval(CompletableFuture<Collection<Row>> resultFuture, Object... keys) {
        JsonArray inputParams = new JsonArray();
        Arrays.asList(keys).forEach(inputParams::add);

        jdbcClient.getConnection(conn -> {
            if (conn.failed()) {
                resultFuture.completeExceptionally(conn.cause());
                return;
            }
            final SQLConnection connection = conn.result();
            String sqlCondition = getSqlFromStatement(tableName, fieldNames, connectionField);
            log.info(String.format("sql查询命令:%s ,参数：%s",sqlCondition,inputParams.getString(0)));
            // vertx异步查询
            connection.queryWithParams(sqlCondition, inputParams, rs -> {
                if (rs.failed()) {
                    resultFuture.completeExceptionally(rs.cause());
                    return;
                }

                int resultSize = rs.result().getResults().size();
                if (resultSize > 0) {
                    List<Row> rowList = Lists.newArrayList();
                    for (JsonArray line : rs.result().getResults()) {
                        Row row = buildRow(line);
                        rowList.add(row);
                    }
                    resultFuture.complete(rowList);
                } else {
                    resultFuture.complete(Collections.emptyList());
                }

                // and close the connection
                connection.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    }
                });
            });
        });
    }

    private Row buildRow(JsonArray line) {
        Row row = new Row(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            row.setField(i, line.getValue(i));
        }
        return row;
    }

    //  数据返回类型
    @Override
    public TypeInformation<Row> getResultType() {
        return  new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        // 使用vertx来实现异步jdbc查询
        JsonObject mysqlClientConfig = new JsonObject();
        mysqlClientConfig.put("url", mysqlConfig.url)
                .put("driver_class", mysqlConfig.jdbcDriver)
                .put("user", mysqlConfig.username)
                .put("password", mysqlConfig.password);
        System.setProperty("vertx.disableFileCPResolving", "true");

        VertxOptions vo = new VertxOptions();
        vo.setFileResolverCachingEnabled(false);
        vo.setWarningExceptionTime(60000);
        vo.setMaxEventLoopExecuteTime(60000);
        Vertx vertx = Vertx.vertx(vo);
        jdbcClient = JDBCClient.createNonShared(vertx, mysqlClientConfig);
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
    //  构建查询维表使用的sql
    private static String getSqlFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
        String fromClause = Arrays.stream(selectFields).map(MysqlAsyncLookupFunction::quoteIdentifier).collect(Collectors.joining(", "));
        String whereClause = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=? ").collect(Collectors.joining(", "));
        String sqlStatement = "SELECT " + fromClause + " FROM " + quoteIdentifier(tableName) + (conditionFields.length > 0 ? " WHERE " + whereClause : "");
        return sqlStatement;
    }


    @Override
    public void close() throws Exception {
        jdbcClient.close();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public Set<FunctionRequirement> getRequirements() {
        return null;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
    //  属性构建
    public static final class Builder {
        // 查询的维表表名
        private String tableName;
        // 查询维表中的字段
        private String[] fieldNames;
        // 查询条件,where中的条件
        private String[] connectionField;
        // 维表数据返回的类型
        private TypeInformation[] fieldTypes;

        private Builder() {
        }

        public static Builder getBuilder() {
            return new Builder();
        }

        public Builder withTableNames(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withConnectionField(String[] connectionField) {
            this.connectionField = connectionField;
            return this;
        }

        public Builder withFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public MysqlAsyncLookupFunction build() {
            return new MysqlAsyncLookupFunction(tableName,fieldNames,connectionField, fieldTypes);
        }
    }

}