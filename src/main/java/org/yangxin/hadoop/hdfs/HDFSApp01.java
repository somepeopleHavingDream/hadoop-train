package org.yangxin.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.yangxin.hadoop.hdfs.mapper.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * 使用HDFS API完成wordcount统计
 * <p>
 * 需求：统计HDFS上的文件的wc，然后将统计结果输出到HDFS
 * <p>
 * 功能拆解：
 * 1）读取HDFS上的文件 ==> HDFS API
 * 2）业务处理（词频统计）：对文件中的每一行数据都要进行业务处理（按照分隔符分割） ==> Mapper
 * 3）将处理结果缓存起来 ==> Context
 * 4）将结果输出到HDFS ==> HDFS API
 *
 * @author yangxin
 * 2023/6/23 22:46
 */
public class HDFSApp01 {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Properties properties = ParamUtil.getProperties();
        String inputPath = properties.getProperty(Constant.INPUT_PATH);
        String hdfsUri = properties.getProperty(Constant.HDFS_URI);
        String outputPath = properties.getProperty(Constant.OUTPUT_PATH);
        String outputFile = properties.getProperty(Constant.OUTPUT_FILE);
        String hdfsUsername = properties.getProperty(Constant.HDFS_USERNAME);
        String mapperClass = properties.getProperty(Constant.MAPPER_CLASS);

        // 1）读取HDFS上的文件 ==> HDFS API
        Path input = new Path(inputPath);

        // 获取要操作的HDFS文件系统
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration(), hdfsUsername);
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(input, false);

        Class<?> clazz = Class.forName(mapperClass);
        Mapper mapper = (Mapper) clazz.newInstance();

        Context context = new Context();

        while (remoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = remoteIterator.next();
            FSDataInputStream fsDataInputStream = fileSystem.open(locatedFileStatus.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // 2）业务处理（词频统计）
                // 在业务逻辑完之后将结果写到Cache中去
                mapper.map(line, context);
            }

            bufferedReader.close();
            fsDataInputStream.close();
        }

        // 3 将结果缓存起来
        Map<Object, Object> contextMap = context.getCacheMap();

        // 4）将结果输出到HDFS ==> HDFS API
        Path output = new Path(outputPath);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(output, outputFile));
        // 将第三步缓存中的内容输出到out中去
        for (Map.Entry<Object, Object> entry : contextMap.entrySet()) {
            fsDataOutputStream.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n")
                    .getBytes(StandardCharsets.UTF_8));
        }

        fsDataOutputStream.close();
        fileSystem.close();
    }
}
