package com.ct.consumer.dao;

import com.ct.common.bean.BaseDao;
import com.ct.common.constant.Names;
import com.ct.common.constant.ValueConstant;
import com.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Hbase数据访问对象
 */
public class HBaseDao extends BaseDao {
    //初始化
    public void init() throws Exception{
        start();
        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(),
                "com.ct.comsumer.coprocessor.InsertCalleeCoprocessor",
                ValueConstant.REGION_COUNT,
                Names.CF_CALLER.getValue(),
                Names.CF_CALLEE.getValue());
        end();

    }
    //插入数据
    public void insertData(String value) throws Exception{
        //将通话日志保存到hbase中

        //获取通话数据
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        //创建数据对象
        //rowKey = regionNum + call1 + calltime + call2 + duration
        String rowKey = genRegionNum(call1,calltime) + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration + "_1";
        //主叫用户
        Put put = new Put(Bytes.toBytes(rowKey));

        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());

        put.addColumn(family,Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(family,Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(family,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(family,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(family,Bytes.toBytes("flg"),Bytes.toBytes("1"));

        String calleeRowKey = genRegionNum(call2,calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";
        //被叫用户
//        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
//        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("flg"),Bytes.toBytes("0"));
        //保存数据
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
//        puts.add(calleePut);
        putData(Names.TABLE.getValue(),puts);

    }

    //插入对象
    public void insertData(Calllog log) throws Exception{
        log.setRowKey(genRegionNum(log.getCall1(),log.getCallTime()) + "_" + log.getCall1() + "_" + log.getCallTime() + "_" + log.getCall2() + "_" + log.getDuration());
    }
}
