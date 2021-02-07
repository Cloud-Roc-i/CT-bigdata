package com.ct.consumer;

import com.ct.common.bean.Consumer;
import com.ct.consumer.bean.CalllogConsumer;

/**
 * 启动消费者
 * 使用kafka消费者获取flume采集的数据
 * 将数据存储到Hbase中去
 */
public class BootStrap {
    public static void main(String[] args) throws Exception {
        //创建消费者
        Consumer consumer = new CalllogConsumer();

        //消费数据
        consumer.consume();

        //关闭资源
        consumer.close();
    }
}
