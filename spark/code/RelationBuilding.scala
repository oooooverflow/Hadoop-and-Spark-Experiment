import org.apache.spark.{SparkConf, SparkContext}


object RelationBuilding {
    def main(args: Array[String]): Unit = {
        if(args.length != 2) {
            System.err.println("Usage: RelationBuilding <in> <out>")
            System.exit(1)
        }

        val conf = new SparkConf().setAppName("RelationBuilding")
        val sc = new SparkContext(conf)
        val prefix = "hdfs://localhost:9000"

        val numPartitions = 2
        val text = sc.textFile(prefix + args(0)).map(line => line.replace("<", "").replace(">", ""))
        val total = text.map(line => (line.split("\t")(0).split(",")(0), line.split("\t")(1).toDouble)).reduceByKey(_ + _)
        val relations = text.map(line => (line.split("\t")(0).split(",")(0), (line.split("\t")(0).split(",")(1), line.split("\t")(1).toDouble)))
        val combined = total.join(relations, 2).
                map(line => (line._1, line._2._2._1 + "," + line._2._2._2 / line._2._1 + "|")).
                groupByKey().
                map(line => line._1 + "\t" + line._2.toString().replace("CompactBuffer", "").replace("|, ", "|").replace("(", "[").replace("|)", "]"))

        combined.repartition(numPartitions).saveAsTextFile(prefix + args(1))
    }
}