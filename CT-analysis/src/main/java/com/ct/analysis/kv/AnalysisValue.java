package com.ct.analysis.kv;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分析数据value
 */
public class AnalysisValue implements Writable {
    public AnalysisValue(){

    }
    public AnalysisValue(String sumCall,String sumDuration){
        this.sumCall = sumCall;
        this.sumDuration = sumDuration;
    }
    private String sumCall;
    private String sumDuration;

    public String getSumCall() {
        return sumCall;
    }

    public String getSumDuration() {
        return sumDuration;
    }

    public void setSumCall(String sumCall) {
        this.sumCall = sumCall;
    }

    public void setSumDuration(String sumDuration) {
        this.sumDuration = sumDuration;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(sumCall);
        dataOutput.writeUTF(sumDuration);
    }

    public void readFields(DataInput dataInput) throws IOException {
        sumCall = dataInput.readUTF();
        sumDuration = dataInput.readUTF();
    }
}
