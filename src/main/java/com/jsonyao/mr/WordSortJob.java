package com.jsonyao.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 单词排序：对指定的两列数据进行排序, 首先对第一列按照从小到大排序, 如果第一列相同, 则需要根据第二列按照从大到小排序
 * <p>
 * 3 3
 * 3 1
 * 3 2
 * 2 1
 * 2 2
 * 1 1
 * <p>
 * =>
 * <p>
 * 1->1
 * 2->2
 * 2->1
 * 3->3
 * 3->2
 * 3->1
 *
 * @author yaocs2
 * @since 2022-08-16
 */
public class WordSortJob {

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
            job.setMapperClass(WordSortMapper.class);
            job.setMapOutputKeyClass(TwoIntWritable.class);
            job.setMapOutputValueClass(NullWritable.class);

            // Reduce
            job.setReducerClass(WordSortReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // 提交Job
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class WordSortMapper extends Mapper<LongWritable, Text, TwoIntWritable, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(WordSortMapper.class);

    /**
     * Map函数：<k1, v1> => <{int1, int2}, null>
     *
     * @param k1
     * @param v1
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
        TwoIntWritable k2 = new TwoIntWritable();
        NullWritable v2 = NullWritable.get();
        if (words.length >= 1) {
            k2.setInt1(Integer.valueOf(words[0]));
        }
        if (words.length >= 2) {
            k2.setInt2(Integer.valueOf(words[1]));
        }

        context.write(k2, v2);
    }
}

class WordSortReducer extends Reducer<TwoIntWritable, NullWritable, Text, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(WordSortReducer.class);

    /**
     * Reduce函数：<{int1, int2}, null> => <int1->int2, null>
     *
     * @param k2
     * @param v2s
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(TwoIntWritable k2, Iterable<NullWritable> v2s, Context context) throws IOException, InterruptedException {
        logger.info(String.format("<k2, v2> = <%s, %s>", k2.toString(), v2s.toString()));

        Text k3 = new Text(k2.getInt1() + "->" + k2.getInt2());
        NullWritable v3 = NullWritable.get();

        logger.info(String.format("<k3, v3> = <%s, %s>", k3.toString(), v3.toString()));
        context.write(k3, v3);
    }
}

class TwoIntWritable implements WritableComparable {

    private int int1;
    private int int2;

    public int getInt1() {
        return int1;
    }

    public void setInt1(int int1) {
        this.int1 = int1;
    }

    public int getInt2() {
        return int2;
    }

    public void setInt2(int int2) {
        this.int2 = int2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(int1);
        out.writeInt(int2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.int1 = in.readInt();
        this.int2 = in.readInt();
    }

    @Override
    public int compareTo(Object obj) {
        if (obj == null) {
            return 1;
        }
        if (equals(obj)) {
            return 0;
        }

        // 首先对第一列按照从小到大排序
        TwoIntWritable o = (TwoIntWritable) obj;
        if (int1 != o.getInt1()) {
            return int1 - o.getInt1();
        }
        // 如果第一列相同, 则需要根据第二列按照从大到小排序
        else {
            return o.getInt2() - int2;
        }
    }

    /**
     * int1相等 && int2相等, 对象才相等
     *
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TwoIntWritable that = (TwoIntWritable) o;
        return int1 == that.int1 && int2 == that.int2;
    }

    /**
     * 重写equals, 必须重写hashCode, 否则使用HashMap时, 会出现明明两个对象equals了, 但仍然不能去重,
     * 其原因正是因为hashCode不相等，导致Entry放在了不同的桶上
     * <p>
     * => 即要保证对象equals, 那么hashCode一定相等
     *
     * @return
     */
    @Override
    public int hashCode() {
        return Objects.hash(int1, int2);
    }

    @Override
    public String toString() {
        return "TwoIntWritable{" +
                "int1=" + int1 +
                ", int2=" + int2 +
                '}';
    }
}
