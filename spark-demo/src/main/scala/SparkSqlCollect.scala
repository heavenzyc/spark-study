import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSqlCollect {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSQLDemo")
    sparkConf.setMaster("local")
    val spark = SparkSession.builder().appName("SparkSQLDemo").config(sparkConf).getOrCreate()
    System.setProperty("hadoop.home.dir", "D:\\winutils")
    runJDBCDataSource(spark)
    //    loadDataSourceFromeJson(spark)
    //    loadDataSourceFromeParquet(spark)
    //    runFromRDD(spark)
    spark.stop()
  }

  private def runJDBCDataSource(spark: SparkSession): Unit = {
    val userInfo = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.8.207:13306/dmall_medusa?user=wumart&password=!QAZxsw2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "user_info") //必须写表名
      .load().select("user_id", "group_id")
    val groupIds = List(1, 2, 3)
    for (id <- groupIds) {
      userInfo.filter(" group_id="+id).show()
    }

    val userBase = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.8.207:13306/dmall_medusa?user=wumart&password=!QAZxsw2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "user_base_info") //必须写表名
      .load().select("id", "sex", "education")
    val joinDF = userInfo
      .groupBy("user_id")
      .agg(collect_set("group_id") as "groupIds")
      .join(userBase, userInfo("user_id") === userBase("id"), "inner")
//    joinDF.groupBy("badge_no", "gender").count().show()

  }
}
