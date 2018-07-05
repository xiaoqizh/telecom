# Telecom电话查询

标签（空格分隔）：  HBase Hadoop MapReduce Mysql

---
总体架构为：<br>

![架构图][1]


1. 生产电话数据（日期 主叫 被叫  持续时间 通话时间）
2.  flume数据采集到Kafka 然后消费数据到HBase
3.  Map Reduce程序从HBase产生具体数据到Mysql（MysqlOutputFormat）
4.  spring boot echarts生成前端显示页面

----------
**telecom_producer**  生产数据：比如 17529353996,16669326501,2018-12-06 03:09:59,0007

----------


**telecom_consumer**  消费数据：HBaseConsumer消费Kafka到HBase中
 -  数据清洗整理 消除无用数据
 - HBase rowKey 设计 、 call1 call2 build_time build_time_ts flag  duration；
 - 分区region 尽可能的使同一个联系人的主叫或者被叫都存放在同一个region中 便于
&nbsp;&nbsp;&nbsp;start row 与stop row查询。通过分区个数创建分区号，然后通过分区号与主叫手机号后四位进行hash取模之后即可。避免出现数据倾斜
 - 列蔟设计 主叫列蔟  f1;被叫列蔟 f2  
 
----------


**telecom_analysis**  分析数据：
 - HBase  
 - 数据维度 时间维度、联系人维度
 - 自定义MapReduceFormat
 - nosql 缓存


  [1]: http://chuantu.biz/t6/338/1530716627x-1566687377.png
  # HBase_MapReduce
