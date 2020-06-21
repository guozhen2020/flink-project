package com.github.guozhen.config;

@SuppressWarnings("ALL")
public class AppConstants {

    // 字段之间的分隔符
    public static final String  FIELD_DELIMITER = ",";

    // hdfs文件滚动写入的周期间隔
    public static final String  FILE_ROLLOVER_INTERVAL="file.Rollover.interval";

    public static final String  HADOOP_USER_NAME="hadoop.user.name";

    // hdfs文件写入的路径
    public static final String  HDFS_OUTPUT_PATH="hdfs.output.path";

    // hdfs分区目录的日期格式
    public static final String  HDFS_PATH_DATE_FORMAT="yyyyMMdd";

    // 文件系统来源
    public static final String FLINK_FS_DEFAULT_SCHEME = "flink.fs.default-scheme";

    // checkpoint 保存路径
    public static final String FLINK_CHECKPOINT_URI="flink.checkpoint.uri";

    // 两次 checkpoint 执行的间隔时间
    public static final String FLINK_CHECKPOINT_INTERVAL="flink.checkpoint.interval";

    // 两次 checkpoint 实际执行时最小间隔时间
    public static final String FLINK_CHECKPOINT_MIN_INTERVAL = "flink.checkpoint.min.interval";

    // 分区并行度 和kafka topic分区数对应，不能超过kafka topic 分区数
    public static final String FLINK_PARTITION_PARALLELISM="flink.partition.parallelism";

    // 执行转换操作的并行度
    public static final String FLINK_EXECUTOR_PARALLELISM="flink.executor.parallelism";

    // 需要同步的Mysql表的数据库和表名，逗号分隔
    public static final String MYSQL_DATABASE_TABLE="mysql.database.table";

    // 需要同步的Mysql表的列，逗号分隔
    public static final String MYSQL_TABLE_COLUMNS="mysql.table.columns";

    // 要写入的hbase的库名:表名,，database:htable
    public static final String  HBASE_MYSQL_MAPPING_DB_TABLE="hbase.mysql.mapping.db_table";

    // 要写入的hbase的表rowkey的组成格式，${id}_${name}_${sex}
    public static final String  HBASE_MYSQL_MAPPING_ROWKEY="hbase.mysql.mapping.rowkey";

    // hbase列与mysql列的映射，cf:name,cf:sex
    public static final String  HBASE_MYSQL_MAPPING_COLUMNS="hbase.mysql.mapping.columns";

    // hbase批量插入数据的最大数量
    public static final Integer  HBASE_DATA_SUBMIT_MAX_SIZE= 1000;

    // hbase批量写入的最大延迟
    public static final Long  HBASE_DATA_SUBMIT_DELAY_TIME= 5000L;
}
