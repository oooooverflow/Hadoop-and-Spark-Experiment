import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

class PageRankMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyInfo = new Text();      // 待发送键值对的key
    private Text valueInfo = new Text();    // 待发送键值对的value
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = value.toString().split("\t")[0];      // 人物名
        Double rank = Double.parseDouble(value.toString().split("\t")[1].split("#")[0]); // 旧的rank
        String relations = value.toString().split("\t")[1].split("#")[1];       // 出度列表
        keyInfo.set(name);
        valueInfo.set("#"+relations);
        context.write(keyInfo, valueInfo);  // 发送value值为出度表的键值对
        String[] slist = relations.split("\\[|\\]")[1].split("\\|");    // 对出度表中每一个对象发送rank值影响
        String target = new String();
        Double weight = 0.0;
        Double res = 0.0;
        for(String s : slist) {
            target = s.split(",")[0];   // 指向的人物名
            weight = Double.parseDouble(s.split(",")[1]);   // 边权重
            keyInfo.set(target);
            res = weight*rank;
            valueInfo.set(res.toString());
            context.write(keyInfo, valueInfo);  // 发送键值对，key为受影响人物名，value为造成的影响
        }
    }
}

class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text valueInfo = new Text();
        String relations = new String();
        double sum = 0.0;
        double d = context.getConfiguration().getDouble("d", -1.0); // 获取全局变量d，控制随机游走的概率
        int N = context.getConfiguration().getInt("totalNumber", -1);   // 获取全局变量N，总人物个数
        for(Text t : values) {
            String value = t.toString();
            if(value.startsWith("#")) { // 出度表
                relations = value;
            }
            else {          // rank
                sum += Double.parseDouble(value);
            }
        }
        sum = sum * d;
        sum = sum + (1-d)/N;    // 总贡献值与随机游走结合得到新的rank值
        valueInfo.set(String.format("%.6f", sum)+relations);
        context.write(key, valueInfo);
    }
}

public class PageRank {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: PageRank <in> <out>");
            System.exit(2);
        }
        String input = otherArgs[0];                // 输入目录
        String output = new String();
        int max_iter = 15;
        for(Integer i = 0; i < max_iter; i++) {     // 迭代15轮
            output = otherArgs[1]+ i.toString();    // 输出目录
            Job job = new Job(conf, "PageRank");
            job.getConfiguration().setInt("totalNumber", 1288); // 设置全局变量N
            job.getConfiguration().setDouble("d", 0.88);        // 设置全局变量d
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setPartitionerClass(HashPartitioner.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(5);                   // 设置五个Reducer节点
            FileInputFormat.addInputPath(job, new Path(input)); // 设置输入路径
            FileOutputFormat.setOutputPath(job, new Path(output));  // 设置输出路径
            job.waitForCompletion(true);    // 等待程序运行完毕
            input = output;
        }
    }
}
