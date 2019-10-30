import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


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

class LPMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyInfo = new Text();
    private Text valueInfo = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = value.toString().split("\t")[0];
        String postings = value.toString().split("\t")[1];
        String label = postings.split("#")[0];

        String relations = "#"+postings.split("#")[1];
        keyInfo.set(name);
        valueInfo.set(relations);
        context.write(keyInfo, valueInfo);
        String[] slist = relations.split("\\[|\\]")[1].split("\\|");

        for (String s : slist) {
            String target = s.split(",")[0];
            String weight = s.split(",")[1];
            keyInfo.set(target);

            valueInfo.set(label + "|" + weight);
            context.write(keyInfo, valueInfo);
        }
    }
}

class LPReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String name = key.toString();
        HashMap<String, Double> neighbor = new HashMap<>();
        String list = "";
        for (Text t : values) {
            //context.write(key, t);
            String temp = t.toString();
            if (temp.startsWith("#")) {
                list = temp.substring(1);
            } else {
                String n = temp.split("\\|")[0];
                Double w = Double.parseDouble(temp.split("\\|")[1]);
                if (!neighbor.containsKey(n)) {
                    neighbor.put(n, w);
                } else {
                    Double m = neighbor.get(n) + w;
                    neighbor.put(n, m);
                }
            }
        }

        String label = "";
        Double max = 0.0;
        for (Map.Entry<String, Double> entry : neighbor.entrySet()) {
            String k = entry.getKey();
            Double v = entry.getValue();
            if (v > max) {
                max = v;
                label = k;
            }
        }



        String res = label + "#" + list;
        context.write(key, new Text(res));

    }
}

public class LabelPropagation {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int maxIteration = 20;
        //String labeled = "陈家洛:0,袁承志:1,郭靖:2,苗人凤:3,胡斐:4,李文秀:5,张无忌:6,杨过:7,萧半和:8,狄云:9,段誉:10,石破天:11,令狐冲:12,韦小宝:13,范蠡:14";
        for (int i = 0; i < maxIteration; i++) {
            Job job = Job.getInstance(conf, "LabelPropagation");
            job.setJarByClass(LabelPropagation.class);
            job.setMapperClass(LPMapper.class);
            job.setReducerClass(LPReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(4);
            //job.getConfiguration().set("labeled", labeled);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0] + Integer.toString(i)));
            Path path = new Path(otherArgs[1] + Integer.toString(i+1));
            FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
            }
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + Integer.toString(i+1)));

            job.waitForCompletion(true);
        }
    }
}
