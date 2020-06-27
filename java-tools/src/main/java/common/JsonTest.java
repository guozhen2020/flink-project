package common;

import com.alibaba.fastjson.JSONArray;

public class JsonTest {
    public static void main(String[] args) {
        //1. 创建JSONArray对象
        String json = "[{\"name\":\"张三\",\"code\":\"123\"},{\"name\":\"李四\",\"code\":\"123\"}]";
        JSONArray jsonArray= JSONArray.parseArray(json);
        System.out.println("jsonArray: "+jsonArray);
        System.out.println();

        //2. JSONArray转String
        String jsonString = jsonArray.toString();
        System.out.println("JSONArray转String: "+jsonString);
        System.out.println();
    }
}
