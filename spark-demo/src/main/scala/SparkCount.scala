import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by heaven.zyc on 2018/7/15.
  */
object SparkCount {
  def main(args: Array[String]): Unit = {
    val logFile = "F:\\my-project\\spark-study\\README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }

}
