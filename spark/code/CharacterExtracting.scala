import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.StringTokenizer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.ansj.splitWord.analysis.DicAnalysis
import org.ansj.library.DicLibrary
import java.net.URI


object CharacterExtracting {
    def main(args: Array[String]): Unit = {
        if(args.length != 2) {
            System.err.println("Usage: ExtractCharacters <in> <out>")
            System.exit(1)
        }

        val conf = new SparkConf().setAppName("ExtractCharacters")
        val sc = new SparkContext(conf)
        val prefix = "hdfs://localhost:9000"

        val fs = FileSystem.get(new URI(prefix), sc.hadoopConfiguration)
        val fileStatuses = fs.listStatus(new Path(prefix + args(0)))
        if (!fs.exists(new Path(prefix + args(1))))
            fs.mkdirs(new Path(prefix + args(1)))

        val inputStream = new FileInputStream("/Users/yanglihe/Desktop/MapReduce/data/people_name_list.txt")
        val bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        var line = ""
        var allNames = ""
        while ({line = bufferedReader.readLine(); line != null}) allNames += line + " "
        inputStream.close()
        bufferedReader.close()

        val itr = new StringTokenizer(allNames)
        while ({itr.hasMoreTokens}) DicLibrary.insert(DicLibrary.DEFAULT, itr.nextToken, "CharacterName", 1000)

        for (i <- Range(0, fileStatuses.length)) {
            val numPartitions = 1
            val text = sc.textFile(prefix + args(0) + "/" + fileStatuses(i).getPath().getName(), numPartitions).map { line =>
                val wordList = DicAnalysis.parse(line)
                var names = ""
                for(i <- Range(0,wordList.size()) if wordList.get(i).getNatureStr() == "CharacterName" && !names.contains(wordList.get(i).getName())) {
                    names += wordList.get(i).getName() + " "
                }
                names
            }.filter(line => line != "")

            text.saveAsTextFile(prefix + args(1) + "/" + fileStatuses(i).getPath().getName())
        }

        for (i <- Range(0, fileStatuses.length)) {
            fs.rename(new Path(prefix + args(1) + "/" + fileStatuses(i).getPath().getName() + "/part-00000"),
                new Path(prefix + args(1) + "/" + fileStatuses(i).getPath().getName().replace(".txt", "")))
            fs.delete(new Path(prefix + args(1) + "/" + fileStatuses(i).getPath().getName()), true)
        }
    }
}