package com.ct.producer.io;

import com.ct.common.bean.Data;
import com.ct.common.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 本地文件数据输入
 */
public class LocalFileDataIn implements DataIn {
    private BufferedReader reader = null;

    public LocalFileDataIn(String path){
        setPath(path);
    }

    public void setPath(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));

        } catch (UnsupportedEncodingException e){
            e.printStackTrace();
        } catch (FileNotFoundException e){
            e.printStackTrace();
        }
    }

    public Object read() throws IOException {
        return null;
    }

    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {
        List<T> ts = new ArrayList<T>();

        try {
            //从数据文件中读取所有数据
            String line = null;
            while ( (line = reader.readLine() ) != null){
                //将数据转换为指定类型的对象，封装为集合返回
                T t = clazz.newInstance();
                t.setValue(line);
                ts.add(t);
            }
        }catch (Exception e){
            e.printStackTrace();
        }


        return ts;
    }

    public void close() throws IOException {
        if(reader!=null){
            reader.close();
        }
    }
}