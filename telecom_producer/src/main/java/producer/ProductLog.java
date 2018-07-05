package producer;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:40 2018/6/28
 * @Description:
 */

public class ProductLog {
    /**
    存放电话号码
     ctrl alt v 快速生成
     */
    private List<String> phoneList = new ArrayList<>();

    /**
     * 手机号与姓名映射
     */
    private Map<String, String> phoneNameMap = new HashMap<>();

    private String startTime = "2017-01-10";
    private String endTime = "2018-12-10";

    /**
     * 初始化数据
     */
    private void initPhoneList() {
        phoneList.add("18916524936");
        phoneList.add("17861185331");
        phoneList.add("19154216931");
        phoneList.add("14016382873");
        phoneList.add("13299296786");
        phoneList.add("16635828479");
        phoneList.add("14203536289");
        phoneList.add("17529353996");
        phoneList.add("16669326501");
        phoneList.add("15799304496");
        phoneList.add("17193116449");
        phoneList.add("14211360832");
        phoneList.add("17404627206");
        phoneList.add("17235818572");
        phoneList.add("15102610525");
        phoneList.add("16318769379");
        phoneList.add("14609745045");
        phoneList.add("14692845781");
        phoneList.add("18755595519");
        phoneList.add("17076258047");
        phoneNameMap.put("18916524936","李雁");
        phoneNameMap.put("17861185331","卫艺");
        phoneNameMap.put("19154216931","仰莉");
        phoneNameMap.put("14016382873","陶欣悦");
        phoneNameMap.put("13299296786","施梅梅");
        phoneNameMap.put("16635828479","金虹霖");
        phoneNameMap.put("14203536289","魏明艳");
        phoneNameMap.put("17529353996","华贞");
        phoneNameMap.put("16669326501","华啟倩");
        phoneNameMap.put("15799304496","仲采绿");
        phoneNameMap.put("17193116449","卫丹");
        phoneNameMap.put("14211360832","戚丽红");
        phoneNameMap.put("17404627206","何翠柔");
        phoneNameMap.put("17235818572","钱溶艳");
        phoneNameMap.put("15102610525","钱琳");
        phoneNameMap.put("16318769379","缪静欣");
        phoneNameMap.put("14609745045","焦秋菊");
        phoneNameMap.put("14692845781","吕访琴");
        phoneNameMap.put("18755595519","沈丹");
        phoneNameMap.put("17076258047","褚美丽");

    }

    /**
     * 生产数据
     */
    public String product() {
        String callerPhone = "";
        String callerName = "";
        String calleePhone = "";
        String calleeName = "";
        int callerIndex;
        int calleeIndex;
        try {
            callerIndex = (int) (Math.random() * phoneList.size());
            callerPhone = phoneList.get(callerIndex);
            callerName = phoneNameMap.get(callerPhone);
            while (true) {
                calleeIndex = (int) (Math.random() * phoneList.size());
                calleePhone = phoneList.get(calleeIndex);
                calleeName = phoneNameMap.get(calleePhone);
                if (!calleePhone.equals(callerPhone)) { break; }
            }
        } catch (Exception e) {

        }
        /*
        然后在获得随机时间
         */
        String buildTime = productRandomTime(startTime, endTime);
        DecimalFormat df = new DecimalFormat("0000");
        String durationTime = df.format(3600 * Math.random());
//        String res =  + calleePhone + "," + buildTime + "," + durationTime;
        StringBuilder sb = new StringBuilder();
        sb.append(callerPhone).append(",").append(calleePhone).append(",").append(buildTime).append(",").append(durationTime);
        return sb.toString();
    }
    /**
     * 随机产生时间
     */
    private String productRandomTime(String startTime, String endTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat dateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formatAns = "";
        try {
            Date startDate = sdf.parse(startTime);
            Date endDate = sdf.parse(endTime);
            long rand = startDate.getTime() + (long) ((endDate.getTime() - startDate.getTime()) * Math.random());
            Date resDate = new Date(rand);
            formatAns = dateTime.format(resDate);
            return formatAns;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return formatAns;
    }

    private String productRandomTimeBy8(String startTime, String endTime) {
//        Instant instant = Instant
        //todo 这个留作作业
        return "";
    }

    /**
     * 写入到文件
     */
    private void writeToFile(String fileName) throws InterruptedException {
        try {
            OutputStreamWriter opw = new OutputStreamWriter(new FileOutputStream(fileName), "UTF-8");
            while (true) {
                Thread.sleep(500);
                String product = product();
                opw.write(product+"\n");
                System.out.println(product);
                opw.flush();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) throws InterruptedException {
        if (args == null || args.length <= 0) {
            System.out.println("no arguments");
            return;
        }

        ProductLog productLog = new ProductLog();
        productLog.initPhoneList();
//        System.out.println(productLog.productRandomTime("2017-01-10", "2018-12-10"));
//        productLog.writeToFile("D:\\tel.csv");
        productLog.writeToFile(args[0]);

    }
}
