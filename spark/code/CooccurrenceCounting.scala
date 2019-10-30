import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.rdd.RDDFunctions._


object CooccurrenceCounting {
    def main(args: Array[String]): Unit = {
        if(args.length != 2) {
            System.err.println("Usage: CooccurrenceCounting <in> <out>")
            System.exit(1)
        }

        val conf = new SparkConf().setAppName("CooccurrenceCounting")
        val sc = new SparkContext(conf)
        val prefix = "hdfs://localhost:9000"

        val numPartitions = 10
        val text = sc.textFile(prefix + args(0)).
                flatMap(line => line.split(" ")).
                map(_.trim).toList).
                flatMap(_.combinations(2)).
                map((_, 1)).
                reduceByKey( _ + _ ).
                map(x => "<" + x._1._1 + "," + x._1._2 + ">" + "\t" + x._2)

        text.repartition(numPartitions).saveAsTextFile(prefix + args(1))
    }
}
