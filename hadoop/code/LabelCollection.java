import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;


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


class LCMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = value.toString().split("\t")[0];  //获取人名
        String postings = value.toString().split("\t")[1];
        String label = postings.split("#")[0];  //获取标签
        context.write(new Text(label), new Text(name));
    }
}

/*class LCPartitioner extends HashPartitioner<Text, Text> {

    public int getPartition(Text key, Text value, int numReduceTasks) {
        //String term = key.toString().split(":")[0];//获取word:filename中的word

        return key.toString().hashCode() % numReduceTasks ;
    }
}*/

class LCReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text v : values) {
            context.write(v, key);
        }
    }
}

public class LabelCollection {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //String labeled = "陈家洛:0,袁承志:1,郭靖:2,苗人凤:3,胡斐:4,李文秀:5,张无忌:6,周威信:7,萧半和:8,狄云:9,段誉:10,石破天:11,令狐冲:12,韦小宝:13,阿青:14";

        Job job = new Job(conf, "LabelCollection");
        job.setJarByClass(LabelCollection.class);
        //使用InvertedIndexMapper类完成Map过程；
        job.setMapperClass(LCMapper.class);

        //使用InvertedIndexPartitioner类完成Partitioner过程；
        //job.setPartitionerClass(LCPartitioner.class);
        //使用InvertedIndexReducer类完成Reducer过程；
        job.setReducerClass(LCReducer.class);
        //设置了Map过程和Reduce过程的输出类型，其中设置key的输出类型为Text；
        job.setOutputKeyClass(Text.class);
        //设置了Map过程和Reduce过程的输出类型，其中设置value的输出类型为Text；
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(10);
        //job.getConfiguration().set("labeled", labeled);
        //设置任务数据的输入路径；
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //设置任务输出数据的保存路径；
        Path path = new Path(otherArgs[1]);
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //调用job.waitForCompletion(true) 执行任务，执行成功后退出；
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
