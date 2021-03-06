package com.ct.common.bean;

import java.io.Closeable;

/**
 * 生产者接口
 */
public interface Producer extends Closeable {
    public void setIn(DataIn in);
    public void setOut(DataOut out);

    public void produce();  //生产数据
}
