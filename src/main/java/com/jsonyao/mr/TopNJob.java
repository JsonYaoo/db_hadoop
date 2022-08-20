package com.jsonyao.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 求TopN：使用MapReduce程序从一批数据中计算出数值最大的前5个数字。
 *
 * <p>
 * 2
 * 1
 * 10
 * 4
 * 8
 * 5
 * 7
 * 6
 * 3
 * 9
 * <p>
 * =>
 * <p>
 * 10
 * 9
 * 8
 * 7
 * 6
 * <p>
 * 任务要求:
 * 1：需要动态支持控制返回TopN元素的个数
 * 2：MapReduce是分布式程序，需要对数据实现全局排序
 *
 * @author yaocs2
 * @since 2022-08-16
 */
public class TopNJob {

    /**
     * 组装Job = Map + Reduce
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.exit(100);
        }

        String fileInputPath = args[0];
        String fileOutputPath = args[1];
        String topN = args[2];

        try {
            Configuration conf = new Configuration();
            conf.set("topN", topN);
            Job job = Job.getInstance(conf);
            job.setJarByClass(TopNJob.class);// 必须设置, 否则提交到集群后, Job会找不到这个WordCountJob类的

            // 输入、输出
            FileInputFormat.setInputPaths(job, new Path(fileInputPath));
            FileOutputFormat.setOutputPath(job, new Path(fileOutputPath));

            // Map
            job.setMapperClass(TopNJobMapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(NullWritable.class);

            // Reduce
            job.setReducerClass(TopNReducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(1);// 全局排序

            // 提交Job
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class TopNJobMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(TopNJobMapper.class);

    /**
     * Map函数：<k1, v1> => <k2, v2>
     *
     * @param k1
     * @param v1
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        logger.info("<k1, v1> = <{}, {}>", k1.get(), v1);

        // 切割字符串
        String[] words = v1.toString().split(" ");

        // <k1, v1> => <k2, v2>
        for (String word : words) {
            LongWritable k2 = new LongWritable(Long.parseLong(word));
            NullWritable v2 = NullWritable.get();
            context.write(k2, v2);
        }
    }
}

class TopNReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(TopNReducer.class);

    private long topN;
    private AtomicLong counter;
    private ConcurrentLinkedDeque<Object[]> deque;

    /**
     * run(..)运行前
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.info("run(..) before...");
        super.setup(context);

        this.topN = context.getConfiguration().getLong("topN", 5);
        this.counter = new AtomicLong(0);
        this.deque = new ConcurrentLinkedDeque<>();
    }

    /**
     * topN入队
     *
     * @param k2
     * @param v2s
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(LongWritable k2, Iterable<NullWritable> v2s, Context context) throws IOException, InterruptedException {
        long size = getSize(v2s);
        logger.info("reduce: <k2, v2s#size> = <{}, {}>", k2.toString(), size);

        // 大于等于topN, 则从左先出队：以减少内存占用, 但最后一个入队时, 如果有重复, 那么要输出的实际个数, 可能会多于topN
        while (counter.get() >= topN && !deque.isEmpty()) {
            Object[] objects = deque.pollFirst();
            long key = (long) objects[0];
            long value = (long) objects[1];

            logger.info("reduce pollFirst deque: <key, value> = <{}, {}>", key, value);
            counter.addAndGet(-value);
        }

        // 再入队
        logger.info("reduce offerLast deque: <k2, v2s#size> = <{}, {}>", k2.toString(), size);
        deque.offerLast(new Object[]{k2.get(), size});
        counter.addAndGet(size);
    }

    /**
     * 获取值列表长度
     *
     * @param v2s
     */
    private long getSize(Iterable<NullWritable> v2s) {
        long size = 0;
        for (NullWritable v2 : v2s) {
            size++;
        }
        return size;
    }

    /**
     * run(..)运行后: <k2, {null,...}> => <k3, null>
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("run(..) after: <k2, {null,...}> => <k3, null>...");
        super.cleanup(context);

        // 从右往左出队, 同时对输出元素限制topN个, 保证结果的正确性
        long rest = this.topN;
        while (!deque.isEmpty() && rest > 0) {
            Object[] objects = deque.pollLast();
            long key = (long) objects[0];
            long value = (long) objects[1];

            for (int i = 0; i < value; i++) {
                logger.info("cleanup: <k2, v2> = <{}, {}>", key, value);

                LongWritable k3 = new LongWritable(key);
                NullWritable v3 = NullWritable.get();

                logger.info("cleanup: <k3, v3> = <{}, {}>", k3.toString(), v3.toString());
                context.write(k3, v3);
                rest--;
            }
        }
    }
}
