import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.net.URI;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;


class CharacterMapper extends Mapper<Object, Text, Text, NullWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String names = "";
        String nameList = context.getConfiguration().get("NameList");
        StringTokenizer itr = new StringTokenizer(nameList);
        while (itr.hasMoreTokens()) {
            DicLibrary.insert(DicLibrary.DEFAULT, itr.nextToken(), "CharacterName", 1000);
        }

        Result parse = DicAnalysis.parse(value.toString());
        List<Term> terms = parse.getTerms();
        for (Term term : terms) {
            if (term.getNatureStr().equals("CharacterName") && !names.contains(term.getName())) {
                names += term.getName() + " ";
            }
        }

        if (!names.equals("")) {
            context.write(new Text(names), NullWritable.get());
        }
    }
}


class CharacterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}


class CharacterLoader {
    public String load(String filename) throws IOException {
        FileInputStream inputStream = new FileInputStream(filename);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line, allNames = "";
        while((line = bufferedReader.readLine()) != null) {
            allNames += line + " ";
        }

        inputStream.close();
        bufferedReader.close();

        return allNames;
    }
}


public class CharacterExtracting {
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: CharacterExtracting <in> <out>");
            System.exit(2);
        }

        String nameFile = "../data/people_name_list.txt";
        CharacterLoader loader = new CharacterLoader();
        String allNames = loader.load(nameFile);

        FileSystem fs = FileSystem.get(URI.create("hdfs://master01:9000"), conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(otherArgs[0]));
        if (!fs.exists(new Path(otherArgs[1]))) {
            fs.mkdirs(new Path(otherArgs[1]));
        }

        for (int i = 0; i < fileStatuses.length; i++) {
            Job job = Job.getInstance(conf, "CharacterExtracting");
            job.setJarByClass(CharacterExtracting.class);
            job.setMapperClass(CharacterMapper.class);
            job.setReducerClass(CharacterReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.getConfiguration().set("NameList", allNames);
            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/" + fileStatuses[i].getPath().getName()));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/" + fileStatuses[i].getPath().getName()));

            job.waitForCompletion(true);
        }

        for (int i = 0; i < fileStatuses.length; i++) {
            fs.rename(new Path(otherArgs[1] + "/" + fileStatuses[i].getPath().getName() + "/part-r-00000"),
                    new Path(otherArgs[1] + "/" + fileStatuses[i].getPath().getName().replace(".txt", "")));
            fs.delete(new Path(otherArgs[1] + "/" + fileStatuses[i].getPath().getName()), true);
        }
    }
}












