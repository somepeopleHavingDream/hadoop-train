package org.yangxin.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义Mapper处理类
 *
 * @author yangxin
 * 2023/7/17 21:04
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Access>.Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\t");

        // 手机号
        String phone = lines[1];
        // 上行流量
        int length = lines.length;
        long up = Long.parseLong(lines[length - 3]);
        // 下行流量
        long down = Long.parseLong(lines[length - 2]);

        context.write(new Text(phone), new Access(phone, up, down));
    }
}
