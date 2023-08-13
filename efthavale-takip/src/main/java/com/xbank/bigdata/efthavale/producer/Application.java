package com.xbank.bigdata.efthavale.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.util.*;

public class Application {

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"165.232.125.23:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,new StringSerializer().getClass().getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,new StringSerializer().getClass().getName());
        Producer producer = new KafkaProducer<String,String>(properties);

        DataGenerator dg = new DataGenerator();

        while (true){
            Thread.sleep(500);
            String data = dg.generate();
            ProducerRecord producerRecord = new ProducerRecord<String,String>("efthavale",data);
            System.out.println(data);
            producer.send(producerRecord);

        }
    }

}
