package com.noodles.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description: TODO
 * @author: liuxian
 * @date: 2023-10-17 21:38
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1, 获取一行
        String line = value.toString();

        // 2, 切割
        String[] words = line.split(" ");

        // 3, 循环写出
        for (String word : words) {
            outK.set(word);
            context.write(outK, outV);
        }
    }
}
