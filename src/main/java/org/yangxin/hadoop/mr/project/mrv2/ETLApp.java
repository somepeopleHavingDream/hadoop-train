package org.yangxin.hadoop.mr.project.mrv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.yangxin.hadoop.mr.project.utils.ContentUtils;
import org.yangxin.hadoop.mr.project.utils.LogParser;

import java.io.IOException;
import java.util.Map;

/**
 * @author yangxin
 * 2023/7/23 15:41
 */
public class ETLApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("input/etl");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ETLApp.class);

        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("input/raw/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private LogParser logParser;

        @Override
        protected void setup(Context context) {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> info = logParser.parse(log);

            String ip = info.get("ip");
            String country = info.get("country");
            String province = info.get("province");
            String city = info.get("city");
            String url = info.get("url");
            String time = info.get("time");
            String pageId = ContentUtils.getPageId(url);

            String builder = ip + "\t" +
                    country + "\t" +
                    province + "\t" +
                    city + "\t" +
                    url + "\t" +
                    time + "\t" +
                    pageId;

            context.write(NullWritable.get(), new Text(builder));
        }
    }
}
