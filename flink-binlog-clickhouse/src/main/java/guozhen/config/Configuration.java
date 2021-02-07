package guozhen.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.Serializable;

@Slf4j
public class Configuration implements Serializable {
    private String path =this.getClass().getClassLoader().getResource("application.conf").getPath();
    protected Config config = ConfigFactory.parseFile(new File(path));
}
