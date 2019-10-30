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

class GraghBuilderMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyInfo = new Text();      // 待发送key
    private Text valueInfo = new Text();    // 待发送value
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = value.toString().split("\t")[0];      // 人物名
        String relations = value.toString().split("\t")[1]; // 出度表
        int N = context.getConfiguration().getInt("totalNumber", -1);   // 获取N
        keyInfo.set(name);
        Double initRank = 1.0/N;                // 初始化rank值
        valueInfo.set(initRank.toString()+"#"+relations);
        context.write(keyInfo, valueInfo);
    }
}

class GraghBuilderReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text t : values) {
            context.write(key, t);  // 无需处理，直接发送
        }
    }
}

public class GraghBuilder {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: GraghBuilder <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "GraghBuilder");
        job.setJarByClass(GraghBuilder.class);          // 指定各个配置参数
        job.setMapperClass(GraghBuilderMapper.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(GraghBuilderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.getConfiguration().setInt("totalNumber", 1288); // 设置全局变量N
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));      // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));    // 输出路径
        job.waitForCompletion(true);
    }
}
