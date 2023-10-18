package com.noodles.mapreduce.writable;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @description: TODO
 * @author: liuxian
 * @date: 2023-10-18 08:15
 */
public class FlowDriver {

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        // 1, 获取 job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2, 设置 jar
        job.setJarByClass(FlowDriver.class);

        // 3, 关联 mapper 和 reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4, 设置 mapper 的输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5, 设置最终数据输出的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交 job
        job.waitForCompletion(true);
    }

}
