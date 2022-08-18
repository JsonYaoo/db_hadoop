package com.jsonyao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * HDFS 源码分析
 *
 * @author yaocs2
 * @since 2022-08-15
 */
public class HdfsOpTest {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream fos = fileSystem.create(new Path("/user.txt"));
        fos.write("hello world!".getBytes());
        fos.close();
    }
}
