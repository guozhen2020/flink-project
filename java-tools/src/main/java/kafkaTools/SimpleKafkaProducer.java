package kafkaTools;

import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SimpleKafkaProducer {
    private static KafkaProducer<String, String> producer;
//    private final static String TOPIC = "saleen_vehicle_bigdata_condition_topic";
private final static String TOPIC = "test";

    public SimpleKafkaProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
//        props.put("bootstrap.servers", "static-node1:9092,static-node2:9092,static-node3:9092");
//        props.put("bootstrap.servers", "kafka1.dev.autoai.com:9092,kafka2.dev.autoai.com:9092,kafka3.dev.autoai.com:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        //configure the following three settings for SSL Encryption
//        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
//        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "D:\\文档\\运营统计\\生产环境\\长城大数据生产环境各服务信息\\ssl\\client.truststore.jks");
//        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "mapbar");
//
//        // configure the following three settings for SSL Authentication
//        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "D:\\文档\\运营统计\\生产环境\\长城大数据生产环境各服务信息\\ssl\\client.keystore.jks");
//        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "mapbar");
//        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "mapbar");

        //设置分区类,根据key进行数据分区
        producer = new KafkaProducer<String, String>(props);
    }
    private void produce(){
        int cnt = 0;
        for (int i =0;i<1;i++){
            List<String> data = mockData();
//        List<String> data =readFile("D:\\ulogstandard.txt");
            for (String d:data) {
                String key = "test";
                producer.send(new ProducerRecord<>(TOPIC,key,d));
                cnt ++;
            System.out.println(String.format("第 %d 条数据发送成功",cnt));
            }
        }

        System.out.println("全部发送成功！");
        producer.close();
    }
    private List<String> mockData(){
        List<String> dataList= new ArrayList<String>();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
        String dateStr = now.format(format);

        dataList.add("{\"data\":[{\"id\":\"1\",\"name\":\"郭振\",\"age\":\"29\",\"sex\":\"1\",\"addr\":\"北京海淀\"},{\"id\":\"2\",\"name\":\"赵奎\",\"age\":\"29\",\"sex\":\"1\",\"addr\":\"北京海淀\"},{\"id\":\"3\",\"name\":\"张忠欣\",\"age\":\"31\",\"sex\":\"1\",\"addr\":\"北京朝阳\"},{\"id\":\"4\",\"name\":\"李辛觉2\",\"age\":\"27\",\"sex\":\"1\",\"addr\":\"北京海淀\"}],\"database\":\"test\",\"es\":1592381570000,\"id\":3,\"isDdl\":false,\"mysqlType\":{\"id\":\"int\",\"name\":\"varchar(255)\",\"age\":\"int\",\"sex\":\"varchar(255)\",\"addr\":\"varchar(2000)\"},\"old\":[{\"sex\":\"男\"},{\"sex\":\"男\"},{\"sex\":\"男\"},{\"sex\":\"男\"}],\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":4,\"name\":12,\"age\":4,\"sex\":12,\"addr\":12},\"table\":\"user_info\",\"ts\":1592381570824,\"type\":\"UPDATE\"}");
//        dataList.add("{\"data\":[{\"orderId\":\"20060100000259575DZKQ1\",\"userId\":\"53a36562d58d4d50b2131718d5d41907\",\"carId\":\"e341cde8-46d3-4b77-b774-178935c94cb7\"}],\"database\":\"testDB\",\"es\":1590913013000,\"id\":\"2\",\"isDdl\":false,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(20)\",\"age\":\"int(11)\",\"addr\":\"varchar(200)\"},\"sql\":\"\",\"sqlType\":{\"id\":\"4\",\"name\":\"12\"},\"table\":\"t1\",\"ts\":15909132990000,\"type\":\"INSERT\"}");
        return  dataList;
    }
//    private List<String> mockData(){
//        List<String> dataList= new ArrayList<String>();
//        LocalDateTime now = LocalDateTime.now();
//        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
//        String dateStr = now.format(format);
//        dataList.add("{\"data\":[{\"id\":\"2\",\"name\":\"张三\",\"age\":\"23\",\"addr\":\"河北\",\"create_date\":\""+dateStr+"\"}],\"database\":\"testDB\",\"es\":1590913013000,\"id\":\"2\",\"isDdl\":false,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(20)\",\"age\":\"int(11)\",\"addr\":\"varchar(200)\"},\"sql\":\"\",\"sqlType\":{\"id\":\"4\",\"name\":\"12\"},\"table\":\"t1\",\"ts\":15909132990000,\"type\":\"INSERT\"}");
//        dataList.add("{\"data\":[{\"id\":\"2\",\"name\":\"张三\",\"age\":\"23\",\"addr\":\"河北\",\"create_date\":\""+dateStr+"\"}],\"database\":\"testDB2\",\"es\":1590913013000,\"id\":\"2\",\"isDdl\":false,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(20)\",\"age\":\"int(11)\",\"addr\":\"varchar(200)\"},\"sql\":\"\",\"sqlType\":{\"id\":\"4\",\"name\":\"12\"},\"table\":\"t1\",\"ts\":15909132990000,\"type\":\"INSERT\"}");
//        dataList.add("{\"data\":[{\"id\":\"2\",\"name\":\"张三\",\"age\":\"23\",\"addr\":\"河北\",\"create_date\":\""+dateStr+"\"}],\"database\":\"testDB\",\"es\":1590913013000,\"id\":\"2\",\"isDdl\":false,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(20)\",\"age\":\"int(11)\",\"addr\":\"varchar(200)\"},\"sql\":\"\",\"sqlType\":{\"id\":\"4\",\"name\":\"12\"},\"table\":\"t1\",\"ts\":15909132990000,\"type\":\"INSERT\"}");
//        return  dataList;
//    }

    public static void main(String[] args) {
//        List<String> a = readFile("D:\\ulogstandard.txt");
//        System.out.println(a.size());
        new SimpleKafkaProducer().produce();
    }

    public static List<String> readFile(String fileName) {
        List<String> output = new ArrayList<>();
        File file = new File(fileName);
        if(file.exists()){
            if(file.isFile()){
                try{
                    BufferedReader input = new BufferedReader (new FileReader(file));
                    String line;
                    while((line = input.readLine()) != null)
                    output.add(line);
                }
                catch(IOException ioException){
                    System.err.println("File Error!");
                }
            }
            else if(file.isDirectory()){
                String[] dir = file.list();
                for(int i=0; i<dir.length; i++){
                    System.out.println(dir[i] +"/n") ;
                }
            }
        }
        else{
            System.err.println("Does not exist!");
        }
        return output;
    }
}