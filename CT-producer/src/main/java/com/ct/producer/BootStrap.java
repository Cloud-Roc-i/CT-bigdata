package com.ct.producer;

import com.ct.common.bean.Producer;
import com.ct.producer.bean.LocalFileProducer;
import com.ct.producer.io.LocalFileDataIn;
import com.ct.producer.io.LocalFileDataOut;

/**
 * 启动对象
 */
public class BootStrap {
    public static void main(String[] args)throws Exception {
        if(args.length < 2){
            System.out.println("系统参数不正确");
            System.exit(1);
        }

        //构建生产者对象
        Producer producer = new LocalFileProducer();
//        producer.setIn(new LocalFileDataIn("E:\\项目学习\\辅助文档\\contact.log"));
//        producer.setOut(new LocalFileDataOut("E:\\项目学习\\辅助文档\\call.log"));
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));


        //生产数据
        producer.produce();

        //关闭生产者对象
        producer.close();

    }
}
