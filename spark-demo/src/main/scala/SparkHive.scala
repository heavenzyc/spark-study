import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf();
    conf.setAppName("SparkSQL2Hive for scala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)*/
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", "spark-warehouse").enableHiveSupport().getOrCreate()
    spark.sql("SELECT * FROM sparktest.student").show()
  }
}
