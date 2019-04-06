import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkFile {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mySpark")
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    conf.setMaster("local")
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val textFile = spark.read.textFile("/home/zyc/devTools/spark241/README.md")
    import spark.implicits._
    var map = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count()
    map.show()
//    println(map)
  }
}
