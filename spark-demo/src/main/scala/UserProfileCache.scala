import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object UserProfileCache {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    println("warehouseLocation=" + warehouseLocation)
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    var current = new DateTime();
    val userInfo = spark.sql("SELECT user_id, group_id FROM dmall_medusa.user_info ").where(" dt = '20180717' and group_key in ('3111', '3200','3217')" ).toDF()
    val userBaseInfoDF = userInfo.groupBy("user_id").agg(collect_set("group_id")).join(spark.sql("SELECT user_id, sex, age, education FROM ads_data.user_profile"), Seq("user_id"), "inner").select("user_id", "sex", "age", "education").cache()
    val groupIds = List("3111", "3200", "3217")
    for (a <- groupIds) {
      val groupInfo = spark.sql("SELECT user_id, group_id FROM dmall_medusa.user_info ").filter("dt='20180717' and group_key="+a)
      val joinDF = groupInfo.join(userBaseInfoDF, groupInfo("user_id") === userBaseInfoDF("user_id"), "inner").select(groupInfo("user_id"), groupInfo("group_id"), userBaseInfoDF("sex"), userBaseInfoDF("age"), userBaseInfoDF("education"))
      groupInfo.unpersist() // 释放缓存
      joinDF.groupBy("sex").count().show()
      joinDF.groupBy("age").count().show()
      joinDF.groupBy("education").count().show()
      joinDF.unpersist() // 释放缓存
    }
    userBaseInfoDF.unpersist() // 释放缓存
    //    joinDF.groupBy("sex").count().show()
    //    joinDF.groupBy("education").count().show()
    println("计算时长==============================" + (new DateTime().getMillis - current.getMillis))
  }

}
