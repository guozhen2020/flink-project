package com.github.guozhen.config;

@SuppressWarnings("ALL")
public class HBaseConstants {

    // hbase master地址，localhost:60010
    public static final String  HBASE_MASTER = "hbase.master";

    // hbase链接的zookeeper地址，多个逗号隔开，127.0.0.1
    public static final String  HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

    // hbase链接的zookeeper地址的端口号，2181
    public static final String  HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT="hbase.zookeeper.property.clientPort";

    // hbase在zookeeper中的根目录，/hbase-unsecure
    public static final String  HBASE_ZOOKEEPER_ZNODE_PARENT="hbase.zookeeper.znode.parent";

    public static final int HBASE_RPC_TIMEOUT=30000;

    public static final int HBASE_CLIENT_OPERATION_TIMEOUT=30000;

    public static final int HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD=30000;

}
