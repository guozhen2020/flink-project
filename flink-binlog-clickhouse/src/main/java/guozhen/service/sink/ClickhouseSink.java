package guozhen.service.sink;

import guozhen.config.ClickhouseConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

public class ClickhouseSink extends RichSinkFunction<Row> implements Serializable {
    private ClickhouseConfig conf= ClickhouseConfig.getInstance();
    private String drivername;
    private String username;
    private String password;
    private String tablename;
    private String database;
    private String url;
    private String[] urls=null;
    private List<Connection> connectionList = new ArrayList<>();
    private List<Statement> statementList = new ArrayList<>();
    private List<PreparedStatement> preparedStatementList = new ArrayList<>();
    private String insertSqlSchema;
    private List<String> columnNameAndType=new ArrayList<>();
    // 插入的批次
    private int insertCkBatchSize = 1000;
    private long lastInsertTime = 0L;
    private long insertCkTimenterval = 4000L;
    private List<Row> list = new ArrayList<>();


    public ClickhouseSink( ClickhouseConfig conf,String database, String tablename) {
        this.drivername = conf.jdbcDriver;
        this.username = conf.username;
        this.password = conf.password;
        this.tablename=tablename;
        this.database=database;
        this.url= conf.urls;

    }

    // 插入数据
    private void insertData(List<Row> rows, PreparedStatement ps, Connection connection) throws SQLException {
        for (Row row : rows) {
            ps = generatePreparedCloumns(ps, this.columnNameAndType, row);
            ps.addBatch();
        }

        ps.executeBatch();
        connection.commit();
        ps.clearBatch();
    }

    /**
     * 动态拼装sql值
     *
     * @param ps
     * @param columnNamesAndType 字段类型
     * @return
     */
    private PreparedStatement generatePreparedCloumns(PreparedStatement ps, List<String> columnNamesAndType,Row row) throws SQLException{

        int y = 0;
        for (int i=0;i <columnNamesAndType.size();i++) {

            // 设置sql值时的下标
            y = i + 1;
            String columnName = columnNamesAndType.get(i).split(":")[0];
            String value = row.getField(i).toString();
            String clickhouseType = columnNamesAndType.get(i).split(":")[1];
            if (value.equals("null") || value.equals("")) {
                value = "null";
            }
            if (isNullable(clickhouseType)){
                clickhouseType = unwrapNullable(clickhouseType);
            }

            if (clickhouseType.startsWith("Int") || clickhouseType.startsWith("UInt")) {
                if (value.equals("null")) {
                    value = "0";
                }
                if (clickhouseType.endsWith("64")) {
                    ps.setLong(y, Long.valueOf(value));
                }
                else ps.setInt(y, Integer.valueOf(value));
            }
            else if ("String".equals(clickhouseType)) {
                if (value.equals("null")) {
                    value="null";
                }
                ps.setString(y, String.valueOf(value));
            }
            else if (clickhouseType.startsWith("Float32")) {
                if (value.equals("null")) {
                    value = "0";
                }
                ps.setFloat(y, Float.valueOf(value));
            }
            else if (clickhouseType.startsWith("Float64")) {
                if (value.equals("null")) {
                    value = "0";
                }
                ps.setDouble(y, Double.valueOf(value));
            }
            else if ("Date".equals(clickhouseType)) {
                if (value.equals("null")) {
                    value = "0";
                }
                ps.setString(y, String.valueOf(value));
            }
            else if ("DateTime".equals(clickhouseType)) {
                if (value.equals("null")) {
                    value = "0";
                }
                ps.setString(y, String.valueOf(value));
            }
            else if ("FixedString".equals(clickhouseType)) { // BLOB 暂不处理
                ps.setString(y, "ERROR");
            }
            else if(isArray(clickhouseType)) {
            }
            else {
                ps.setString(y, "ERROR");
            }
        }

        return ps;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        Class.forName(this.drivername);

        // 创建连接
        for (int i=0;i<this.urls.length;i++) {
            Connection connection = DriverManager.getConnection(urls[i], this.username, this.password);
            connection.setAutoCommit(false);
            this.connectionList.add(connection);
            Statement statement = connection.createStatement();
            this.statementList.add(statement);

            if(i==0){
                // 动态获取表字段:类型
                columnNameAndType = getColumnNameAndType(this.tablename, this.database, connection);
                for (String item : columnNameAndType) {
                    System.out.println(item);
                }
                // 插入语句
                insertSqlSchema = generatePreparedSqlByColumnNameAndType(columnNameAndType, this.database, this.tablename);
                System.out.println(insertSqlSchema);
            }
            PreparedStatement preparedStatement = connection.prepareStatement(insertSqlSchema);
            this.preparedStatementList.add(preparedStatement);
        }
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {

        // 随机写入各个local表，避免单节点数据过多
        if (null != row) {
            Random random = new Random();
            int index = random.nextInt(this.urls.length);
            if (list.size() >= this.insertCkBatchSize || isTimeToDoInsert()) {
                insertData(list, preparedStatementList.get(index), connectionList.get(index));
                list.clear();
                this.lastInsertTime = System.currentTimeMillis();
            } else {
                list.add(row);
            }
        }
    }

    /**
     * 动态封装sql语句
     * @param columnNames 字段列表
     * @param database 库名
     * @param tableName 表名
     * @return 插入语句
     */
    private String generatePreparedSqlByColumnNameAndType(List<String> columnNames,String database,String tableName) throws SQLException {
        StringBuilder insertColumns = new StringBuilder();
        StringBuilder insertValues = new StringBuilder();
        if (columnNames != null && columnNames.size() > 0) {
            insertColumns.append(columnNames.get(0).split(":")[0]);
            insertValues.append("?");
        }

        for (String columnName : columnNames) {
            insertColumns.append(", ").append(columnName.split(":")[0]);
            insertValues.append(", ?") ;
        }
        return "INSERT INTO " + database + "." + tableName + " (" + insertColumns + ") values(" + insertValues + ")";
    }

    /**
     *根据库名/表名获取字段名称及类型
     * @param tableName 表名
     * @param database 库名
     * @param conn
     * @return 类型集合
     * @throws SQLException
     */
    private List<String> getColumnNameAndType(String tableName, String database,Connection conn) throws SQLException{
        DatabaseMetaData dd = conn.getMetaData();
        List<String> columnNameAndType = new ArrayList<>();
        ResultSet colRet = dd.getColumns(null, "%", tableName, "%");
        while(colRet.next()) {
            String columnName = colRet.getString("COLUMN_NAME");
            String columnType = colRet.getString("TYPE_NAME");
            columnNameAndType.add(columnName + ":" + columnType);
        }

        return columnNameAndType;
    }


    private void insertData(Row row,List<Row> list,int index) throws SQLException{

        if (list.size() >= this.insertCkBatchSize || isTimeToDoInsert()) {
            insertData(list, preparedStatementList.get(index), connectionList.get(index));
            list.clear();
            this.lastInsertTime = System.currentTimeMillis();
        } else {
            list.add(row);
        }
    }
    @Override
    public void close() throws Exception {

        for (Statement statement : this.statementList) {
            if (null != statement) {
                statement.close();
            }
        }

        for (PreparedStatement preparedStatement : this.preparedStatementList) {
            if (null != preparedStatement) {
                preparedStatement.close();
            }
        }

        for (Connection connection : this.connectionList) {
            if (null != connection) {
                connection.close();
            }
        }
    }

    /**
     * 根据时间判断是否插入数据
     *
     * @return
     */
    private boolean isTimeToDoInsert() {
        long currTime = System.currentTimeMillis();
        return currTime - this.lastInsertTime >= this.insertCkTimenterval;
    }

    /**
     * 判断是否为Nullable
     *
     * @param clickhouseType
     * @return
     */
    private Boolean isNullable(String clickhouseType){
        return  clickhouseType.startsWith("Nullable(") && clickhouseType.endsWith(")");
    }

    private String unwrapNullable(String clickhouseType){
        return clickhouseType.substring("Nullable(".length(), clickhouseType.length() - 1);
    }

    /**
     * 判断是否为Array
     *
     * @param clickhouseType
     * @return
     */
    private Boolean isArray(String clickhouseType ) {
        return clickhouseType.startsWith("Array(") && clickhouseType.endsWith(")");
    }
}