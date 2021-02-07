package service;

import guozhen.config.ClickhouseConfig;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.sql.*;
import java.util.*;

public class ClickhouseSinkTest {

    private ClickhouseConfig  conf= ClickhouseConfig.getInstance();
    private String drivername = conf.jdbcDriver;
    private String username = conf.username;
    private String password = conf.password;
    private String tablename="test_user";
    private String database="default";
    private String[] urls = conf.urls.split(",");
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


    @Test
    public void open() throws Exception{
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

    /**
     * 动态封装sql语句
     * @param columnNames 字段列表
     * @param database 库名
     * @param tableName 表名
     * @return 插入语句
     */
    private String generatePreparedSqlByColumnNameAndType(List<String> columnNames, String database, String tableName) throws SQLException {
        StringBuilder insertColumns = new StringBuilder();
        StringBuilder insertValues = new StringBuilder();

        for (int i=0;i<columnNames.size();i++) {
            String columnName = columnNames.get(i).split(":")[0];
            if(i==0){
                insertColumns.append(columnName);
                insertValues.append("?");
            }
            else {
                insertColumns.append(", ").append(columnName);
                insertValues.append(", ?") ;
            }

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
    private List<String> getColumnNameAndType(String tableName, String database, Connection conn) throws SQLException{
        DatabaseMetaData dd = conn.getMetaData();
        List<String> res = new ArrayList<>();
        ResultSet colRet = dd.getColumns(null, database, tableName, "%");
        while(colRet.next()) {
            String columnName = colRet.getString("COLUMN_NAME");
            String columnType = colRet.getString("TYPE_NAME");
            res.add(columnName + ":" + columnType);
        }

        return res;
    }


    /**
     * 动态拼装sql值
     *
     * @param ps
     * @param columnNamesAndType 字段类型
     * @return
     */
    private PreparedStatement generatePreparedCloumns(PreparedStatement ps, List<String> columnNamesAndType,Row row) throws Exception{

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

    @Test
    public void createTable() throws Exception{
        String createSql=
                "CREATE TABLE IF NOT EXISTS test_user( " +
                "  user_id UInt64," +
                "  score String," +
                "  deleted UInt8 DEFAULT 0," +
                "  create_time DateTime DEFAULT toDateTime(0)" +
                ")ENGINE= ReplacingMergeTree(create_time)" +
                "ORDER BY user_id";

        open();
        PreparedStatement preparedStatement=this.preparedStatementList.get(0);
        int isSuccess = preparedStatement.executeUpdate(createSql);
        System.out.println("create table is success ："+isSuccess);

    }

    public void write(Row row) throws Exception{
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

    private void insertData(List<Row> rows, PreparedStatement ps, Connection connection) throws Exception {
        for (Row row : rows) {
            ps = generatePreparedCloumns(ps, this.columnNameAndType, row);
            ps.addBatch();
        }

        ps.executeBatch();
        connection.commit();
        ps.clearBatch();
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

    private List<Row> mockData(int cnt){
        List<Row> resultSet = new ArrayList<>();
        Random random = new Random();
        for(int i=1; i< cnt;i++){
            Row row = new Row(4);
            row.setField(0,i);
            row.setField(1,random.nextInt(1000));
            row.setField(2,random.nextInt(2));
            row.setField(3,"2020-09-01 20:26:54");
            resultSet.add(row);
        }

        return resultSet;
    }

    @Test
    public void testMockData(){
        mockData(10);
    }

    @Test
    public void testInsert() throws Exception{
        List<Row> list = mockData(2345);
        open();
        long start = System.currentTimeMillis();
        for (Row r: list ) {
            write(r);
        }
        long end = System.currentTimeMillis();
        System.out.println("耗时："+(end - start));
    }

    @Test
    public void test_(){
        Object value = 2;
        System.out.println(Long.valueOf(value.toString()));
    }
}
