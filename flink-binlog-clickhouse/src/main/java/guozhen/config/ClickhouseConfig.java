package guozhen.config;

import com.typesafe.config.Config;

public class ClickhouseConfig extends Configuration {
    private static ClickhouseConfig ourInstance = new ClickhouseConfig();

    public static ClickhouseConfig getInstance() {
        return ourInstance;
    }

    private ClickhouseConfig() {
    }
    private final Config appConfig = config.getConfig("clickhouse");
    public final String  jdbcDriver ="ru.yandex.clickhouse.ClickHouseDriver";
    public final String  urls = appConfig.getString("urls");
    public final String  username = appConfig.getString("username");
    public final String  password = appConfig.getString("password");

    @Override
    public String toString(){
        return String.format("ClickhouseConfig : {jdbcDriver : %s, urls : %s, username : %s, password : %s}"
                ,jdbcDriver,urls,username,password);
    }
}
