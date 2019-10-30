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

class GetResultMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyInfo = new Text();  // 待发送键值对的key
    private Text valueInfo = new Text();// 待发送键值对的value
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = value.toString().split("\t")[0];  // 人物名
        Double rank = Double.parseDouble(value.toString().split("\t")[1].split("#")[0]);    // rank值
        keyInfo.set(name);
        valueInfo.set(rank.toString());
        context.write(keyInfo, valueInfo);
    }
}

class GetResultReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text t : values) {
            context.write(key, t);  // 不做处理直接发送
        }
    }
}

public class GetResult {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: GetResult <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "GetResult");
        job.setJarByClass(GetResult.class);     // 设置配置文件信息
        job.setMapperClass(GetResultMapper.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(GetResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(5);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  // 设置输入路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
