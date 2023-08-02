package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
public class KafkaKb12Producer {
    public static void main(String[] args) throws IOException,ExecutionException,InterruptedException{
        Properties config = new Properties();
        //设置连接的集群
        config.setProperty("bootstrap.servers","192.168.131.200:9092");
        //设置容错
        config.setProperty("retries","2");
        config.setProperty("acks","-1");
        //批处理 满足任意一条都会推送消息
        config.setProperty("batch.size", "128");
        config.setProperty("linger.ms", "100");
        //消息键值的序列化
        config.setProperty("key.serializer","org.apache.kafka.common.serialization.LongSerializer");
        config.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Long, String> producer = new KafkaProducer<Long, String>(config);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        long count = 0;
        final String TOPIC = "kb12_01"; //发送的主题
        final int PARTITION = 0; //指定主题的分区

        while (true){
            String input = reader.readLine();
            if(input.equalsIgnoreCase("exit")){
                break;
            }
            ProducerRecord<Long,String> record = new ProducerRecord<Long,String>(TOPIC, PARTITION,++count,input);
            RecordMetadata rmd = producer.send(record).get();
            System.out.println(rmd.topic()+"\t"+rmd.partition()+"\t"+rmd.offset()+"\t"+count+":"+input);
        }
        reader.close();
        producer.close();
    }
}
