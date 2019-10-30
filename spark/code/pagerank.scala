import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)
    if(args.length != 2) {
      System.err.println("Usage: PageRank <in> <out>")
      System.exit(1)
    }
    var nameAndRank = sc.textFile(args(0)).map(line => (line.split("\t")(0),1.0/1288))
    val relations = sc.textFile(args(0)).map(line => (line.split("\t")(0),line.split("\t")(1).split("\\[|\\]")(1).split("\\|").toSeq))
    //val nameAndRank = names.map((_, 1.0/1288))
    val iter_num = 15
    for(i <- 1 to iter_num) {
      val contribs = nameAndRank.join(relations,2).flatMap {
        case(_, (rank,links)) => links.map(dest => (dest.split(",")(0), dest.split(",")(1).toDouble*rank.toDouble))
      }
      nameAndRank = contribs.reduceByKey(_ + _,2).mapValues(0.15/1288 + 0.85 * _)
    }
    val sorts = nameAndRank.sortBy(_._2,false)
    sorts.repartition(1).saveAsTextFile(args(1))
  }
}
