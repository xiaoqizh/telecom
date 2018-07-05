package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:05 2018/6/30
 * @Description: 被叫写入 协处理器
 */

public class CalleeWriteObserver /*extends BaseRegionObserver*/ {
   /* *//**
     * 客户端访问之前
     * 如果直接返回 return  那么用户根本就不能访问到这张表
     *//*
*//*    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {

        super.preOpen(e);
    }*//*

    *//**
     *  保存之后调用
     *  ObserverContext 保存所有的上下文信息
     *  put 之前成功保存之后的put 是同一个内容
     *
     *  需要注意的是  很多个表 都可能用这一个协处理器
     *//*
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        //获取要操作的目标表的名称
        String targetTable = PropertiesUtil.getProperty("hbase.calllog.tablename");
        //得到当前表名
        //一定要注意api的使用
        String regionNameAsString = e.getEnvironment().getRegionInfo().getTable().getNameAsString();
        if (!regionNameAsString.equals(targetTable)) {
            return;
        }
        //得到rowKey
        String oriRowKey = Bytes.toString(put.getRow());
        String[] infos = oriRowKey.split("_");
        String caller = infos[1];
        String callee = infos[3];
        String buildTime = infos[2];
        String duration = infos[5];
        String oldFlag = infos[4];
        *//*
        这个函数执行完之后 也会重新再次调用这个函数
         *//*
        if (oldFlag.equals("0")) {
            return;
        }
        String flag = "0";
        int regionCount = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regioncount"));
        String regionCode = HBaseUtil.genRegionCode(callee, buildTime, regionCount);
        String calleeRowKey = HBaseUtil.genRowKey(regionCode, callee, buildTime, caller, flag, duration);
        //put中的是 rowKey
        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        //这里可能会出现异常
        long buildTimeTs= 0;
        try {
            buildTimeTs = sdf.parse(buildTime).getTime();
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
        //f1中放主叫 放在f2列蔟中f2中是被叫
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        Table tableInterface = e.getEnvironment().getTable(TableName.valueOf(targetTable));
        tableInterface.put(calleePut);
        //这里table就不是自己创建的
        tableInterface.close();
    }*/
}
