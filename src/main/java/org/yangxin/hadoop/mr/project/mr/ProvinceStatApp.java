package org.yangxin.hadoop.mr.project.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.yangxin.hadoop.mr.project.utils.IPParser;
import org.yangxin.hadoop.mr.project.utils.LogParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * @author yangxin
 * 2023/7/23 15:02
 */
public class ProvinceStatApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(args[1]);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ProvinceStatApp.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final LongWritable ONE = new LongWritable(1);

        private LogParser logParser;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> info = logParser.parse(log);
            String ip = info.get("ip");

            if (StringUtils.isBlank(ip)) {
                context.write(new Text("-"), ONE);
                return;
            }

            IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(ip);
            if (Objects.isNull(regionInfo)) {
                context.write(new Text("-"), ONE);
                return;
            }

            String province = regionInfo.getProvince();
            if (StringUtils.isBlank(province)) {
                context.write(new Text("-"), ONE);
                return;
            }

            context.write(new Text(province), ONE);
        }
    }

    private static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable ignored : values) {
                count++;
            }

            context.write(key, new LongWritable(count));
        }
    }
}
