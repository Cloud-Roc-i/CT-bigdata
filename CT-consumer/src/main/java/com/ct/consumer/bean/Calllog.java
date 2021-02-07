package com.ct.consumer.bean;

import com.ct.common.api.Column;
import com.ct.common.api.RowKey;
import com.ct.common.api.TableRef;

@TableRef("ct:calllog")
public class Calllog {
    @RowKey
    private String rowKey;
    @Column(family = "caller")
    private String call1;
    @Column(family = "caller")
    private String call2;
    @Column(family = "caller")
    private String callTime;
    @Column(family = "caller")
    private String duration;
    @Column(family = "caller")
    private String flg = "1";

    public Calllog(){
    }
    public Calllog(String data){
        String[] values = data.split("\t");
        call1 = values[0];
        call2 = values[1];
        callTime = values[2];
        duration = values[3];
    }
    public String getCall1() {
        return call1;
    }

    public String getCall2() {
        return call2;
    }

    public String getCallTime() {
        return callTime;
    }

    public String getDuration() {
        return duration;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public void setCallTime(String callTime) {
        this.callTime = callTime;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }
}
