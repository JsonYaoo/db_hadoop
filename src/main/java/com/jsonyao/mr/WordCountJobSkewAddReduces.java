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
 * 数据倾斜-解决方案1：增加Reduce个数
 *
 * @author yaocs2
 * @since 2022-08-16
 */
public class WordCountJobSkewAddReduces {

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
        String reduceTaskNum = args[2];

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCountJobSkewAddReduces.class);// 必须设置, 否则提交到集群后, Job会找不到这个WordCountJob类的

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

            // 设置Reduce任务个数(默认为1)
            job.setNumReduceTasks(Integer.parseInt(reduceTaskNum));

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

        private static final Logger logger = LoggerFactory.getLogger(MyMapper.class);

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

            // <k1, v1> => <k2, v2>：只取每行的第一个单词
            Text k2 = new Text(words[0]);
            LongWritable v2 = new LongWritable(1L);
            context.write(k2, v2);
        }
    }

    /**
     * Reduce阶段
     *
     * @author yaocs2
     * @since 2022-08-16
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private static final Logger logger = LoggerFactory.getLogger(MyReducer.class);

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
                logger.info(String.format("<k2, v2> = <%s, %s>", k2.toString(), v2));
                sum += v2.get();

                // 模拟Reduce复杂计算所消耗的时间
                if(sum % 200 == 0) {
                    Thread.sleep(1);
                }
            }

            // <k2, {v2,..}> => <k3, v3>
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            logger.info(String.format("<k1, v1> = <%s, %s>", k3.toString(), v3));
            context.write(k3, v3);
        }
    }
}
