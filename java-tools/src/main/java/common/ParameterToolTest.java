package common;

import org.apache.flink.api.java.utils.ParameterTool;

public class ParameterToolTest {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        System.out.println(parameterTool.get("kafka.group-id"));
    }
}
