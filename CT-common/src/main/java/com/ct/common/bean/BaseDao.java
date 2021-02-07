package com.ct.common.bean;

import com.ct.common.api.Column;
import com.ct.common.api.RowKey;
import com.ct.common.api.TableRef;
import com.ct.common.constant.Names;
import com.ct.common.constant.ValueConstant;
import com.ct.common.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class BaseDao {
    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws Exception {
        getConnection();
        getAdmin();
    }
    protected void end() throws Exception {
        Admin admin = getAdmin();
        if(admin != null){
            admin.close();
            adminHolder.remove();
        }

        Connection conn = getConnection();
        if(conn != null){
            conn.close();
            connHolder.remove();
        }
    }
    //创建命名空间，如果已存在则不需要创建
    protected void createNamespaceNX(String namespace) throws Exception{
        Admin admin = getAdmin();
        try{
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e){
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }

    }

    //创建表，如果已经存在，那么删除后再创建新的
    protected void createTableXX(String name, String... families) throws Exception{
        createTableXX(name,null,null,families);
    }

    protected void createTableXX(String name,String coprocessorClass, Integer regionCount, String... families) throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        if(admin.tableExists(tableName)){
            //表存在，删除表
            deleteTable(name);
        }
        //创建表
        createTable(name,coprocessorClass,regionCount,families);
    }

    private void createTable(String name,String coprocessorClass,Integer regionCount ,String... familyNames)throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);

        if(familyNames ==null || familyNames.length == 0){
            familyNames = new String[1];
            familyNames[0] = Names.CF_INFO.getValue();
        }
        List<ColumnFamilyDescriptor> families = new ArrayList<ColumnFamilyDescriptor>();
        for (String familyName : familyNames) {
            families.add(ColumnFamilyDescriptorBuilder.newBuilder(familyName.getBytes()).build());
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families);
        if(coprocessorClass!=null && !"".equals(coprocessorClass)){
            tableDescriptorBuilder.setCoprocessor(coprocessorClass);
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        //增加预分区
        if (regionCount == null || regionCount <= 1){
            admin.createTable(tableDescriptor);
        } else {
            //分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor,splitKeys);
        }
    }

    /**
     * 计算分区号
     * @param tel
     * @param date
     * @return
     */
    protected int genRegionNum(String tel,String date){
        String userCode = tel.substring(tel.length()-4);
        String yearMonth = date.substring(0,6);

        int userCodeHash = userCode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        //crc校验，采用异或算法
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        //取模
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }

    /**
     * 生成分区键
     * @param regionCount
     * @return
     */
    private byte[][] genSplitKeys(int regionCount){
        int spiltKeyCount = regionCount - 1;
        byte[][] bs = new byte[spiltKeyCount][];

        List<byte[]> bsList = new ArrayList<byte[]>();
        for (int i = 0;i<spiltKeyCount;i++){
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }
        bsList.toArray(bs);

        return bs;

    }

    /**
     * 获取查询时startrow，stoprow集合
     * @return
     */
    protected List<String[]> getStartStopRowkeys(String tel,String start,String end){
        List<String[]> rowkeyss = new ArrayList<String[]>();
        String startTime = start.substring(0,6);
        String endTime = end.substring(0,6);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime,"yyyyMM"));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime,"yyyyMM"));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()){
            String nowTime = DateUtil.format(startCal.getTime(),"yyyyMM");

            int regionNum = genRegionNum(tel,nowTime);
            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String endRow = startRow + "|";

            String[] rowkeys={startRow,endRow};
            rowkeyss.add(rowkeys);

            //月份加一
            startCal.add(Calendar.MONTH,1);
        }


        return rowkeyss;
    }
    //增加多条数据
    protected void putData(String name, List<Put> puts) throws Exception{
        //获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));

        //增加数据
        table.put(puts);

        //关闭表
        table.close();
    }

    //增加数据
    protected void putData(String name, Put put) throws Exception{
        //获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));

        //增加数据
        table.put(put);

        //关闭表
        table.close();
    }

    //增加对象
    protected void putData(Object obj) throws Exception{
        //反射
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef)clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();

        Field[] fs = clazz.getDeclaredFields();
        String stringRowKey="";
        for (Field f : fs) {
            RowKey rowKey = f.getAnnotation(RowKey.class);
            if (rowKey != null ){
                f.setAccessible(true);
                stringRowKey = (String)f.get(obj);
            }
        }
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(stringRowKey));

        for (Field f : fs) {
            Column column = f.getAnnotation(Column.class);
            if(column != null){
                String family = column.family();
                String colName = column.column();
                if(colName == null || "".equals(colName)){
                    colName = f.getName();
                }
                f.setAccessible(true);
                String value = (String)f.get(obj);
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(colName),Bytes.toBytes(value));
            }
        }

        table.put(put);

        table.close();
    }

    //删除表格
    protected void deleteTable(String name) throws Exception{
        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    //获取连接对象
    protected synchronized Connection getConnection() throws Exception{
        Connection conn = connHolder.get();
        if(conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }

    protected synchronized Admin getAdmin() throws Exception{
        Admin admin = adminHolder.get();
        if(admin == null){
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }
}
