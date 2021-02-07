本项目是一个电信客服大数据实战案例，模拟通话日志的产生，并对其进行统计分析。数据为随机产生，仅供学习使用。  

项目主要基于尚硅谷大数据项目教程，教程链接：https://www.bilibili.com/video/BV17t411W7wZ 我自己把代码实现了一遍，并适配我个人使用的软件版本。  
部分代码由于版本问题无法使用，经个人努力已经替换为新版本的API。
如：  
视频中使用的HTableDescriptor在hbase官方文档中说明已弃用  
在这里使用TableDescriptorBuilder代替  
具体代码位置com.ct.common.bean.BaseDao  


开发顺序及各module作用：  

CT-common：通用类和工具类  

CT-producer：用java随机生成通信数据并写入日志文件  

CT-consumer：kafka获取flume采集的数据，并保存到hbase  

CT-consumer-coprocessor：hbase的协处理器，在向hbase中插入主叫用户数据时生成一条被叫用户数据  

CT-analysis：从hbase中获取数据，经过MapReduce分析后输出到MySQL中  

CT-cache：把MySQL中的数据存到redis  

CT-web：用echart进行数据可视化  

注意事项：  

需要把mysql和redis的jar包放进Hadoop中，参考目录/hadoop/share/hadoop/common  

需要把自己打包的CT-coprocessor和CT-common放进hbase中，参考目录/hbase/lib  

需要修改为自己MySQL数据库的地方：  

CT-common/src/main/java/com/ct/common/util/JDBCUtil  

CT-web/src/main/resource/spring-context.xml  

