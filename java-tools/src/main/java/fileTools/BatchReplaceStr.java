package fileTools;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class BatchReplaceStr {

    public static void main(String[] args) {
        //        String path = "D:\\test";
        String path = "D:\\gofun_bi_etl";
        String toPath = "D:\\target";
        Map<String,String> repalceStrMap= new HashMap<>(8);
        repalceStrMap.put("#!/bin/bash","#!/bin/bash\nsource ../config/config.sh");
        repalceStrMap.put(" gofun_ods."," ${ods}.");
        repalceStrMap.put(" gofun_bdm."," ${dwd}.");
        repalceStrMap.put(" gofun_dwd."," ${dwd}.");
        repalceStrMap.put(" gofun_dws."," ${dws}.");
        repalceStrMap.put(" gofun_app."," ${app}.");
        repalceStrMap.put(" bdm."," ${old_bdm}.");
        repalceStrMap.put(" gofun_temp."," ${tmp}.");
        repalceStrMap.put(" gofun_dw."," ${dw}.");
        repalceStrMap.put(" dm.","${dm}.");

        Map<String,String> regexStrMap= new HashMap<>(8);
        regexStrMap.put("options-file .*/","options-file ../config/sqoop_import/");
        regexStrMap.put("beeline[^\"]*\"[^\"]*\"[^\"]*\"","\\${beeline_shell} \"");
        try {
            deal(path,repalceStrMap,regexStrMap,toPath);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private static void deal(String path, Map<String,String> repalceStrMap,Map<String,String> regexStrMap, String toPath) throws IOException {
        File file = new File(path);
        File[] files = file.listFiles();

        assert files != null;
        for (File file1 : files) {
            if(file1.isDirectory()) {
                deal(path + "\\" + file1.getName(), repalceStrMap,regexStrMap,toPath);
            } else {
                if(file1.getName().endsWith(".sh")) {
                    String content = readFile(file1);
                    String newContent = replaceStr(content,repalceStrMap,regexStrMap);
                    writeFile(file1,newContent,toPath);
                }
            }
        }
    }

    private static String readFile(File file1) throws IOException {
        String fromEncoding = FileCharsetDetector.getFileCharset(file1);

        BufferedReader bdf = new BufferedReader(new InputStreamReader(new FileInputStream(file1),fromEncoding));

        String str;
        StringBuilder context = new StringBuilder();
        while((str=bdf.readLine())!=null){
            context.append(str).append("\n");
        }
        bdf.close();
        return context.toString();
    }

    private static String replaceStr(String content,Map<String,String> repalceStrMap,Map<String,String> regexStrMap){
        for (Map.Entry<String,String> entry:repalceStrMap.entrySet()) {
            content=content.replace(entry.getKey(),entry.getValue());
        }
        for (Map.Entry<String,String> entry:regexStrMap.entrySet()) {
            content=content.replaceAll(entry.getKey(),entry.getValue());
        }
        return content;
    }

    private static void writeFile(File file1, String content,String toPath) throws IOException {
        String target = toPath+file1.getParent().substring(2);
        File tarDir = new File(target);
        if(!tarDir.exists()){
            tarDir.mkdirs();
        }
        File targetFile = new File(target+"\\"+file1.getName());
        BufferedWriter bdw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetFile), StandardCharsets.UTF_8));
        bdw.write(content);
        bdw.close();
        log.info("文件："+file1.getPath()+",替换成功！");
//        System.out.println("将" + file1.getPath() + "文件格式从 " + fromEncoding + " 转换为 " + "utf-8");

    }

}
