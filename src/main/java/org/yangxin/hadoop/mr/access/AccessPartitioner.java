package org.yangxin.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MapReduce自定义分区规则
 *
 * @author yangxin
 * 2023/7/17 21:30
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    @Override
    public int getPartition(Text phone, Access access, int numReduceTasks) {
        String phoneString = phone.toString();
        if (phoneString.startsWith("13")) {
            return 0;
        } else if (phoneString.startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
