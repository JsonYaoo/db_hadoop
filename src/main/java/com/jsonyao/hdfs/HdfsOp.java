package com.jsonyao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * JAVA代码操作HDFS：上传文件、下载文件、删除文件
 *
 * @author yaocs2
 * @since 2022-08-14
 */
public class HdfsOp {

    /**
     * org.apache.hadoop.security.AccessControlException: Permission denied:
     * 解决方案：
     *
     *     <property>
     *         <name>dfs.permissions.enabled</name>
     *         <value>false</value>
     *     </property>
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // 获取配置
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");// 即core-site.xml中配置的fs.defaultFS

        // 获取HDFS文件系统对象
        FileSystem fileSystem = FileSystem.get(conf);

        // 上传文件
//        put(fileSystem);

        // 下载文件
        get(fileSystem);

        // 删除文件: true表示递归删除
//        delete(fileSystem);

        // 获取文件所有block块
//        list(fileSystem);
    }

    /**
     * 获取文件所有block块
     *
     * path: hdfs://bigdata01:9000/hadoop-3.2.0.tar.gz
     * 分块数量：3
     * block_0_location: bigdata02	134217728
     * block_0_location: bigdata03	134217728
     * block_1_location: bigdata03	134217728
     * block_1_location: bigdata02	134217728
     * block_2_location: bigdata02	77190019
     * block_2_location: bigdata03	77190019
     * ==========
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void list(FileSystem fileSystem) throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/hadoop-3.2.0.tar.gz"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

            int blockLen = blockLocations.length;
            System.out.println("path: " + fileStatus.getPath().toString());
            System.out.println("分块数量：" + blockLen);

            for (int i = 0; i < blockLen; i++) {
                String[] hosts = blockLocations[i].getHosts();
                long blkLength = blockLocations[i].getLength();
                for(int j = 0; j < hosts.length; j++) {
                    System.out.println("block_" + i + "_location: " + hosts[j] + "\t" + blkLength);
                }
            }

            System.out.println("==========");
        }
    }

    /**
     * 删除文件
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void delete(FileSystem fileSystem) throws IOException {
        if (fileSystem.delete(new Path("/00 test.txt"), true)) {
            System.out.println("删除成功!");
        } else {
            System.out.println("删除失败!");
        }
    }

    /**
     * 下载文件
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void get(FileSystem fileSystem) throws IOException {
        FSDataInputStream fis = fileSystem.open(new Path("/README.txt"));
        FileOutputStream fos = new FileOutputStream("D:\\README.txt");
        IOUtils.copyBytes(fis, fos, 1024, true);
    }

    /**
     * 上传文件
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void put(FileSystem fileSystem) throws IOException {
        FileInputStream fis = new FileInputStream("D:\\Users\\yaocs2\\Desktop\\00 test.txt");
        FSDataOutputStream fos = fileSystem.create(new Path("/00 test.txt"));
        IOUtils.copyBytes(fis, fos, 1024, true);
    }
}
