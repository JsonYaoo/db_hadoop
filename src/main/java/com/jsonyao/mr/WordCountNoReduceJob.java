package com.jsonyao.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 单词计数(无Reduce版)：只显示单词, 不统计
 * <p>
 * hello you
 * hello me
 * <p>
 * =>
 * <p>
 * hello    2
 * me   1
 * you  1
 *
 * @author yaocs2
 * @since 2022-08-18
 */
public class WordCountNoReduceJob {

    /**
     * 组装Job = Map + Reduce
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.exit(100);
        }

        String fileInputPath = args[0];
        String fileOutputPath = args[1];

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCountNoReduceJob.class);// 必须设置, 否则提交到集群后, Job会找不到这个WordCountJob类的

            // 输入、输出
            FileInputFormat.setInputPaths(job, new Path(fileInputPath));
            FileOutputFormat.setOutputPath(job, new Path(fileOutputPath));

            // Map
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // 禁用Reduce
            job.setNumReduceTasks(0);

            // 提交Job
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Map阶段
     *
     * @author yaocs2
     * @since 2022-08-16
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        Logger logger = LoggerFactory.getLogger(MyMapper.class);

        /**
         * Map函数：<k1, v1> => <k2, v2>
         *
         * @param k1      每行数据的行首偏移量
         * @param v1      每行的数据内容
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            logger.info(String.format("<k1, v1> = <%s, %s>", k1.get(), v1));

            // 切割字符串
            String[] words = v1.toString().split(" ");

            // <k1, v1> => <k2, v2>
            for (String word : words) {
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                context.write(k2, v2);
            }
        }
    }
}
