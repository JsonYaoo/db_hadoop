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

import java.io.IOException;

/**
 * 单词计数：统计每个单词出现的总次数
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
 * @since 2022-08-16
 */
public class WordCountJob {

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
            job.setJarByClass(WordCountJob.class);// 必须设置, 否则提交到集群后, Job会找不到这个WordCountJob类的

            // 输入、输出
            FileInputFormat.setInputPaths(job, new Path(fileInputPath));
            FileOutputFormat.setOutputPath(job, new Path(fileOutputPath));

            // Map
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // Reduce
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

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

    /**
     * Reduce阶段
     *
     * @author yaocs2
     * @since 2022-08-16
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        /**
         * Reduce函数：<k2, {v2,..}> => <k3, v3>
         *
         * @param k2      单词的值
         * @param v2s     单词出现的所有次数
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
            // 累加所有次数
            long sum = 0L;
            for (LongWritable v2 : v2s) {
                sum += v2.get();
            }

            // <k2, {v2,..}> => <k3, v3>
            context.write(k2, new LongWritable(sum));
        }
    }
}
