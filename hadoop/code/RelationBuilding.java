import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


class RelationMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString().replace("<", "").replace(">", "");
        String names = str.split("\t")[0];
        int number = Integer.parseInt(str.split("\t")[1]);
        String[] nameList = names.split(",");
        context.write(new Text(nameList[0]), new Text(nameList[1] + " " + number));
    }
}


class RelationReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String newValue = "[";
        double sum = 0.0;

        ArrayList<Double> numbersCache = new ArrayList<>();
        ArrayList<String> namesCache = new ArrayList<>();
        for (Text value : values) {
            double number = Double.parseDouble(value.toString().split(" ")[1]);
            sum += number;
            numbersCache.add(number);
            namesCache.add(value.toString().split(" ")[0]);
        }

        for (int i = 0; i < numbersCache.size(); i++) {
            double percent = numbersCache.get(i) / sum;
            if (i != 0) {
                newValue += "|";
            }
            newValue += namesCache.get(i) + "," + String.format("%.5f", percent);
        }

        newValue += "]";
        context.write(key, new Text(newValue));
    }
}


public class RelationBuilding {
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: RelationBuilding <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "RelationBuilding");
        job.setJarByClass(RelationBuilding.class);
        job.setMapperClass(RelationMapper.class);
        job.setReducerClass(RelationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
