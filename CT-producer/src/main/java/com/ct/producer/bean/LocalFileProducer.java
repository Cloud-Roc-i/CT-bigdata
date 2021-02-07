package com.ct.producer.bean;

import com.ct.common.bean.*;
import com.ct.common.util.DateUtil;
import com.ct.common.util.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {
    private DataIn in;
    private DataOut out;
    private volatile boolean flag = true;

    public void setIn(DataIn in) {
        this.in = in;
    }

    public void setOut(DataOut out) {
        this.out = out;
    }

    public void produce() {
        try {
            //读取通讯录数据
            List<Contact> contacts = in.read(Contact.class);

            while (flag) {

                //从通讯录中随机查找两个电话号码
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                while (true){
                    call2Index = new Random().nextInt(contacts.size());
                    if(call1Index!=call2Index){
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                //生成随机通话时间
                String startDate = "20200101000000";
                String endDate = "20210101000000";

                long startTime = DateUtil.parse(startDate,"yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate,"yyyyMMddHHmmss").getTime();

                //通话时间
                long callTime = startTime + (long)((endTime - startTime) * Math.random());
                String callTimeString = DateUtil.format(new Date(callTime),"yyyyMMddHHmmss");

                //生产随机通话时长
                String duration = NumberUtil.format(new  Random().nextInt(3000),4);

                //生成通话记录
                Calllog calllog = new Calllog(call1.getTel(),call2.getTel(),callTimeString,duration);
                System.out.println(calllog);

                //将通话记录写到数据文件中
                out.write(calllog);

                Thread.sleep(500);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        if(in!=null){
            in.close();
        }
        if(out!=null){
            out.close();
        }
    }


}
