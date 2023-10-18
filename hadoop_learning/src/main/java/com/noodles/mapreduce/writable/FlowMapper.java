package com.noodles.mapreduce.writable;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description: TODO
 * @author: liuxian
 * @date: 2023-10-18 08:15
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1, 获取一行
        String line = value.toString();

        // 2, 切割
        String[] words = line.split("\t");

        // 3, 获取想要的数据
        String phone = words[1];
        String up = words[words.length - 3];
        String down = words[words.length - 2];
        System.out.println("phone: " + phone + " up = " + up + " down = " + down);

        // 4, 封装结果
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();
        context.write(outK, outV);
    }

}
