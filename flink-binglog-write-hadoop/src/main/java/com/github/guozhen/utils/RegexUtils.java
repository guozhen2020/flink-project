package com.github.guozhen.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexUtils {

    private static final String BRACE_REGEX = "\\{([^}])*}";

    public static List<String> matchBrace(String value){

        return match(value,BRACE_REGEX);
    }

    private static List<String> match(String value,String regex){
        List<String> result = new ArrayList<>();
        // 创建 Pattern 对象
        Pattern r = Pattern.compile(regex);

        // 现在创建 matcher 对象
        Matcher matcher = r.matcher(value);
        while (matcher.find()) {
            result.add(matcher.group());
        }
        return result;
    }

    public static void main(String[] args) {
        String line = "${id}_${name}_${sex}";
        matchBrace(line).forEach(t-> System.out.println(t));
    }

}
