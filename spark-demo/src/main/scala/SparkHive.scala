import java.io.File

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object SparkHive {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    println("warehouseLocation=" + warehouseLocation)
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    var current = new DateTime();
    var userInfo = spark.sql("SELECT user_id FROM dmall_medusa.user_info ").where(" dt = '20180717' and group_key = '3111'" ).toDF()
    var joinDF = userInfo.join(spark.sql("SELECT user_id, phone, sex, age, education FROM ads_data.user_profile"), Seq("user_id"), "inner")
    joinDF.groupBy("sex").count().show()
    joinDF.groupBy("education").count().show()
    println("计算时长==============================" + (new DateTime().getMillis - current.getMillis))
  }
}
