package common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexMatchesTest {

    public static void main( String args[] ){

        // 按指定模式在字符串查找
        String line = "${id}_${name}_${sex}";
        String pattern = "\\{([^}])*\\}";

        // 创建 Pattern 对象
        Pattern r = Pattern.compile(pattern);

        int i=1;
        // 现在创建 matcher 对象
        Matcher matcher = r.matcher(line);
        while (matcher.find()) {
            System.out.println(matcher.group());
            line=line.replace("$"+matcher.group(),i+"");
            i++;
        }

        System.out.println(line);
    }
}
