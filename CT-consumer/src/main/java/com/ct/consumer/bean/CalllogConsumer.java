package com.ct.consumer.bean;

import com.ct.common.bean.Consumer;
import com.ct.common.constant.Names;
import com.ct.consumer.dao.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志消费者对象
 */
public class CalllogConsumer implements Consumer {
    public void consume() {
        try {
            //创建配置对象
            Properties prop = new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            //获取flume采集的数据
            KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(prop);
            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

            HBaseDao dao = new HBaseDao();
            dao.init();
            //消费数据
            while(true){
                ConsumerRecords<String,String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    //Calllog log = new Calllog();
                    dao.insertData(consumerRecord.value());
                    //dao.insertData(log);

                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    public void close() throws IOException {

    }
}
