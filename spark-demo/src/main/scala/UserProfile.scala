import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object UserProfile {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    println("warehouseLocation=" + warehouseLocation)
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    var current = new DateTime();
    var userInfo = spark.sql("SELECT user_id, group_id FROM dmall_medusa.user_info ").where(" dt = '20180717' and group_key in ('3111', '3200','3217')" ).toDF().limit(10)
    val joinDF = userInfo.groupBy("user_id").agg(collect_set("group_id")).join(spark.sql("SELECT user_id, phone, sex, age, education FROM ads_data.user_profile"), Seq("user_id"), "inner")
    joinDF.groupBy("sex").count().show()
    joinDF.groupBy("education").count().show()
    println("计算时长==============================" + (new DateTime().getMillis - current.getMillis))
  }

  def toMap(dataFrame: DataFrame): Map[Long, Set[Long]] = {
    var userInfoMap : Map[Long, Set[Long]] = Map()
    for (row <- dataFrame.collect()) {
      var groupSet = userInfoMap.filter((t) => t._1 == row.getAs[Long]("user_id")).get(row.getAs[Long]("user_id"))
      if (groupSet == None) {
        var set : Set[Long] = Set(row.getAs[Long](1))
        userInfoMap += (row.getAs[Long]("user_id") -> set)
      } else {
        var set = groupSet.get
        set += row.getAs[Long]("group_id")
        userInfoMap += (row.getAs[Long]("user_id") -> set)
      }
    }
    return userInfoMap
  }


}
