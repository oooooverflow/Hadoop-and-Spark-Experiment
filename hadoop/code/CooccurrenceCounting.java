import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;


class CooccurrenceMapper extends Mapper<Object, Text, Text, IntWritable> {
    private String cooccurrence;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] names = value.toString().split(" ");
        for (int i = 0; i < names.length; i++) {
            for (int j = 0; j < names.length; j++) {
                if (i != j) {
                    cooccurrence = "<" + names[i] + "," + names[j] + ">";
                    context.write(new Text(cooccurrence), new IntWritable(1));
                }
            }
        }
    }
}


class CooccurrencePartitioner extends HashPartitioner<Text, IntWritable> {
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        String term = key.toString().split(",")[0];
        return super.getPartition(new Text(term), value, numPartitions);
    }
}


class CooccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}


public class CooccurrenceCounting {
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: CooccurrenceCounting <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "CooccurrenceCounting");
        job.setJarByClass(CooccurrenceCounting.class);
        job.setMapperClass(CooccurrenceMapper.class);
        job.setReducerClass(CooccurrenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(10);
        job.setPartitionerClass(CooccurrencePartitioner.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
