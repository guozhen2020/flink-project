package guozhen.config;

import com.typesafe.config.Config;

public class MySqlConfig extends Configuration {
    private static MySqlConfig ourInstance = new MySqlConfig();

    public static MySqlConfig getInstance() {
        return ourInstance;
    }

    private MySqlConfig() {
    }
    private final Config appConfig = config.getConfig("mysql");
    public final String  jdbcDriver = "com.mysql.jdbc.Driver";
    public final String  url = appConfig.getString("url");
    public final String  username = appConfig.getString("username");
    public final String  password = appConfig.getString("password");
}
