package com.github.guozhen.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.Serializable;

@Slf4j
public class Configuration implements Serializable {
    private String path =this.getClass().getClassLoader().getResource("app-binglog-hdfs.conf").getPath();
    protected Config config = ConfigFactory.parseFile(new File(path));
//    private void init(){
//        URL url=this.getClass().getClassLoader().getResource("app-binglog-hdfs.conf");
//        if(url !=null)
//        {
//            String path = url.getPath();
//            config = ConfigFactory.parseFile(new File(path));
//        }
//        else
//        {
//            log.error("The configuration file does not exist. Please check the path");
//            System.exit(-1);
//        }
//    }
}
