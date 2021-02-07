package common;

import java.util.*;

public class commonTest {
    public static void main(String[] args) {
//        Map<String,Integer> columnIndexMap = new HashMap<>(32);
//        System.out.println(columnIndexMap.size());

        List<Long> list= new ArrayList<>();
        list.add(0L);
        list.add(3L);
        list.add(2L);
        list.forEach(t-> System.out.print(t));
        Collections.sort(list);
        list.forEach(t-> System.out.println(t));
    }
}
