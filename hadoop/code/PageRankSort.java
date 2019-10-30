import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
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

class GlobalSortDoubleWritable extends DoubleWritable {
    public GlobalSortDoubleWritable() {
        super();
    }
    public GlobalSortDoubleWritable(Double d) {
        super(d);
    }
    @Override
    public int compareTo(DoubleWritable o) {    // 将返回值置反
        if (this.get() == o.get()) {
            return 0;
        } else if (this.get() > o.get()) {
            return -1;
        } else {
            return 1;
        }
    }
}

class PageRankSortMapper extends Mapper<Object, Text, GlobalSortDoubleWritable, Text> {
    @Override
    protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String name = value.toString().split("\t")[0];  // 人物名
        String rank = value.toString().split("\t")[1];  // rank值
        context.write(new GlobalSortDoubleWritable(Double.parseDouble(rank)), new Text(name));
    }
}

class PageRankSortPartitioner extends HashPartitioner<GlobalSortDoubleWritable, Text> { // 自定义DoubleWritable类
    public int getPartition(GlobalSortDoubleWritable key, Text value, int numReduceTasks) {
        return super.getPartition(key, value, numReduceTasks);
    }
}

class PageRankSortReducer extends Reducer<GlobalSortDoubleWritable, Text, Text, Text>{
    @Override
    protected void reduce(GlobalSortDoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        for(Text t : values) {
            context.write(t, new Text(key.toString())); // 需要将key与value互相调换发送
        }
    }
}

public class PageRankSort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: PageRankSort <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "PageRankSort");
        job.setJarByClass(PageRankSort.class);
        job.setMapperClass(PageRankSortMapper.class);
        job.setPartitionerClass(PageRankSortPartitioner.class);
        job.setReducerClass(PageRankSortReducer.class);
        job.setMapOutputKeyClass(GlobalSortDoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
