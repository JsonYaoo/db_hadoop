package com.jsonyao.mr;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

/**
 * 小文件解决方案之 MapFile
 *
 * @author yaocs2
 * @since 2022-08-19
 */
public class SmallFileMap {

    public static void main(String[] args) throws IOException {
        String hdfsFilePath = "/mapFile";
        // 合并inputDir下所有的小文件, 到HDFS#outputFile文件中
        write("D:\\Users\\yaocs2\\data\\myWorkspace\\imooc_bigdata\\bigdata_course_materials\\hadoop\\mapreduce+yarn\\smallFile", hdfsFilePath);

        // 读取HDFS中合并后的inputFile文件
        read(hdfsFilePath);
    }

    /**
     * 合并inputDir下所有的小文件, 到HDFS#outputFile文件中
     *
     * @param inputDir
     * @param outputDir
     * @throws IOException
     */
    private static void write(String inputDir, String outputDir) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");

        // 先删除HDFS中合并后的文件
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(outputDir), true);

        /**
         * 构造options数组：
         *
         * a. key的类型
         * b. value的类型
         */
        SequenceFile.Writer.Option[] options = new SequenceFile.Writer.Option[]{
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(Text.class)
        };

        MapFile.Writer writer = new MapFile.Writer(conf, new Path(outputDir), options);

        // 读取指定目录下的所有小文件
        File inputDirPath = new File(inputDir);
        if (inputDirPath.isDirectory()) {
            File[] files = inputDirPath.listFiles();
            for (File file : files) {
                String fileName = file.getName();
                String content = FileUtils.readFileToString(file, "UTF-8");

                // 把合并后的文件写到HDFS中
                Text key = new Text(fileName);
                Text value = new Text(content);
                writer.append(key, value);
            }
        }

        // 关闭流
        writer.close();
    }

    /**
     * 读取HDFS中合并后的inputFile文件
     *
     * @param inputDir
     * @throws IOException
     */
    private static void read(String inputDir) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");

        // 创建阅读器
        MapFile.Reader reader = new MapFile.Reader(new Path(inputDir), conf);

        // 创建缓冲区
        Text key = new Text();
        Text value = new Text();

        // 开始读取
        while (reader.next(key, value)) {
            System.out.print(String.format("文件名: %s, ", key.toString()));
            System.out.println(String.format("文件内容: %s", value.toString()));
        }

        // 关闭流
        reader.close();
    }
}