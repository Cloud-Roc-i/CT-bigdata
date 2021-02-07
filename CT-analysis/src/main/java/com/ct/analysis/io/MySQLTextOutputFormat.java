package com.ct.analysis.io;

import com.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL数据格式化输出对象
 */
public class MySQLTextOutputFormat extends OutputFormat<Text,Text> {
    protected static class MySQLRecordWriter extends RecordWriter<Text, Text>{

        private Connection connection = null;
        private Jedis jedis = null;
//        private Map<String,Integer> userMap = new HashMap<String, Integer>();
//        private Map<String,Integer> dateMap = new HashMap<String, Integer>();

        public MySQLRecordWriter(){
            //获取资源
            connection = JDBCUtil.getConnection();
            jedis = new Jedis("localhost",6379);
//已经写进了CT-cache的BootStrap
//            //读取用户、时间数据
//            String queryUserSql = "select id, tel from ct_user";
//            PreparedStatement pstat = null;
//            ResultSet rs = null;
//            try {
//                pstat = connection.prepareStatement(queryUserSql);
//                rs = pstat.executeQuery();
//                while (rs.next()){
//                    Integer id = rs.getInt(1);
//                    String tel = rs.getString(2);
//                    userMap.put(tel,id);
//                }
//                rs.close();
//
//                String queryDateSql = "select id, year, month, day from ct_date";
//                pstat = connection.prepareStatement(queryDateSql);
//                rs = pstat.executeQuery();
//                while (rs.next()){
//                    Integer id = rs.getInt(1);
//                    String year = rs.getString(2);
//                    String month = rs.getString(3);
//                    if(month.length() == 1){
//                        month = "0"+month;
//                    }
//                    String day = rs.getString(4);
//                    if(day.length() == 1){
//                        day = "0"+day;
//                    }
//                    dateMap.put(year+month+day,id);
//                }
//
//            } catch (Exception e){
//                e.printStackTrace();
//            } finally {
//                if(pstat != null) {
//                    if(rs != null){
//                        try {
//                            rs.close();
//                        } catch (SQLException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                    try {
//                        pstat.close();
//                    } catch (SQLException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }

        }

        public void write(Text key, Text value) throws IOException, InterruptedException {
            String[] values = value.toString().split("_");
            String sumCall = values[0];
            String sumDuration = values[1];
            PreparedStatement preparedStatement = null;
            try {
                String insertSQL = "insert into ct_call (telid, dateid, sumcall, sumduration ) values(?,?,?,?)";
                preparedStatement = connection.prepareStatement(insertSQL);

                String k = key.toString();
                String[] ks = k.split("_");
                String tel = ks[0];
                String date = ks[1];

                preparedStatement.setInt(1,Integer.parseInt(jedis.hget("ct_user",tel)));
                preparedStatement.setInt(2,Integer.parseInt(jedis.hget("ct_date",date)));
                preparedStatement.setInt(3,Integer.parseInt(sumCall));
                preparedStatement.setInt(4,Integer.parseInt(sumDuration));
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if(preparedStatement!=null){
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(jedis!=null){
                jedis.close();
            }
        }
    }

    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }
    private FileOutputCommitter committer = null;
    public static Path getOutputPath(JobContext job){
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null: new Path(name);
    }
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if(committer == null) {
            Path output = getOutputPath(taskAttemptContext);
            committer = new FileOutputCommitter(output,taskAttemptContext);
        }
        return committer;
    }
}
