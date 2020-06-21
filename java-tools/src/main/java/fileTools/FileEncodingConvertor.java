package fileTools;

import java.io.*;

public class FileEncodingConvertor {
    public static void main(String[] args) throws IOException {
//        String path = "D:\\test";
        String path = "D:\\workspace\\gofun_bi_etl\\Dw4.0";
        String toPath = "D:\\test\\target";
        String toEncoding = "utf-8";
        getAllJavaDoc(path,toEncoding,toPath);
    }

    private static void getAllJavaDoc(String path, String toEncoding, String toPath) throws IOException {
        File file = new File(path);
        File[] files = file.listFiles();

        assert files != null;
        for (File file1 : files) {
            if(file1.isDirectory()) {
                getAllJavaDoc(path + "\\" + file1.getName(), toEncoding,toPath);
            } else {
                if(file1.getName().endsWith(".sh")) {
                    changeTo(file1,toEncoding,toPath);
                }
            }
        }
    }

    private static void changeTo(File file1, String toEncoding,String toPath) throws IOException {

        String fromEncoding = FileCharsetDetector.getFileCharset(file1);
        //如果源文件编码格式与目标文件相同,则不处理
        if(toEncoding.equalsIgnoreCase(fromEncoding)) return;

        BufferedReader bdf = new BufferedReader(new InputStreamReader(new FileInputStream(file1),fromEncoding));

        String str;
        StringBuilder context = new StringBuilder();
        while((str=bdf.readLine())!=null){
            context.append(str).append("\n");
        }

        File targetFile = new File(toPath+"\\"+file1.getName());
        BufferedWriter bdw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetFile),toEncoding));
        bdw.write(context.toString());
        System.out.println("将" + file1.getPath() + "文件格式从 " + fromEncoding + " 转换为 " + toEncoding);
        bdw.close();
        bdf.close();
    }


}
