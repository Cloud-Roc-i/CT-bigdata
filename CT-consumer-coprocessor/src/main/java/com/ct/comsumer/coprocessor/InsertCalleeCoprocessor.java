package com.ct.comsumer.coprocessor;

import com.ct.common.bean.BaseDao;
import com.ct.common.constant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;

/**
 * 使用协处理器保存被叫用户数据
 * 需要将项目打成jar包发布到hbase中（关联的jar包也要发布）
 */
public class InsertCalleeCoprocessor implements RegionObserver {
    /**
     * 保存用户数据之后由Hbase自动保存被叫用户数据
     * @param c
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        Table table = c.getEnvironment().getConnection().getTable(TableName.valueOf(Names.TABLE.getValue()));

        //主叫用户的rowKey
        String rowKey = Bytes.toString(put.getRow());
        String[] values = rowKey.split("_");

        CoprocessorDao dao = new CoprocessorDao();

        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];
        String flg = values[5];

        if("1".equals(flg)) {
            //只有主叫用户保存才触发被叫用户保存
            String calleeRowKey = dao.getRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";

            Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("flg"), Bytes.toBytes("0"));
            table.put(calleePut);

            table.close();
        }
    }

    private class CoprocessorDao extends BaseDao{
        public int getRegionNum(String tel,String time){
            return genRegionNum(tel,time);
        }
    }
}
