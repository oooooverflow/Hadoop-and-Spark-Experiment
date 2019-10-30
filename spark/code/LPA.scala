import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.Map


object LPA {

def func(a : String, b : String) : String = {
	if (a.split(",")(1).toDouble > b.split(",")(1).toDouble) {
		return a
	} else {
		return b
	}
}
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LPA")
    val sc = new SparkContext(conf)
    if(args.length != 2) {
      System.err.println("Usage: LPA <in> <out>")
      System.exit(1)
    }
    var nameAndLabel = sc.textFile(args(0)).map(line => (line.split("\t")(0),line.split("\t")(0)))
    val relations = sc.textFile(args(0)).map(line => (line.split("\t")(0),line.split("\t")(1).split("\\[|\\]")(1).split("\\|").toSeq))
    //val nameAndRank = names.map((_, 1.0/1288))
    val iter_num = 10
    for(i <- 1 to iter_num) {
      val contribs = nameAndLabel.join(relations,2).flatMap {
        case(_, (label,links)) => links.map(dest => (dest.split(",")(0)+","+label, dest.split(",")(1).toDouble))
      }
	val merge = contribs.reduceByKey(_+_, 2)
	val lalala = merge.map((kk)=>(kk._1.split(",")(0), kk._1.split(",")(1)+","+kk._2.toString) )
	nameAndLabel = lalala.reduceByKey(func(_,_),2).map(vv=>(vv._1, vv._2.split(",")(0)));
	//lalala.saveAsTextFile(args(1))
    }
	//nameAndLabel.saveAsTextFile(args(1))
    val sorts = nameAndLabel.sortBy(_._2,false)
    sorts.repartition(1).saveAsTextFile(args(1))
  }
}
