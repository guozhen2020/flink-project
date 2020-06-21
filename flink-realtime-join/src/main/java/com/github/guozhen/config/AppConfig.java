package com.github.guozhen.config;

import com.typesafe.config.Config;

public class AppConfig extends Configuration {
    private static AppConfig ourInstance = new AppConfig();

    public static AppConfig getInstance() {
        return ourInstance;
    }

    private AppConfig() {
    }
    private final Config appConfig = config.getConfig("application");
    public final String  fieldDelimiter = appConfig.getString("field.delimiter");
    public final Long  fileRolloverInterval = appConfig.getLong("file.Rollover.interval");
    public final String  hdfsOutputPath = appConfig.getString("hdfs.output.path");
    public final String  hdfsPathDateFormat = appConfig.getString("hdfs.path.date.format");
}
