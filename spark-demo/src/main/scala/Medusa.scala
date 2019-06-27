import java.io.File

import org.apache.spark.sql.SparkSession

object Medusa {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    groupUser(spark)
  }

  def groupUser(spark : SparkSession): Unit = {
    var groupInfo = spark.sql("SELECT operate_user_name, operate_user_id, count(1) cnt from dmall_medusa.group_info where created>='2019-02-01 00:00:00' and group_type in (1,2) GROUP BY operate_user_name, operate_user_id order by cnt desc")
    var baseUser = spark.sql("select id, dep_id, jobs_id  from dmall_erp.erp_base_user where user_status=1")
    var dept = spark.sql("select id, dep_name from dmall_erp.erp_dep_info")
    var sta = spark.sql("select id, sta_name from dmall_erp.erp_sta_info")
    var user = groupInfo.join(baseUser, groupInfo("operate_user_id") === baseUser("id"))
    var all = user.join(dept, user("id") === dept("id")).join(sta, user("id") === sta("id"))
    all.select("operate_user_name", "operate_user_id", "cnt", "dep_id", "dep_name", "jobs_id", "sta_name").show()
    all.toDF().registerTempTable("groupUser")
    spark.sql("select * from groupUser").show()
    spark.sql("insert into ana_result.medusa_group_user select operate_user_name, operate_user_id, cnt, dep_id, dep_name, jobs_id, sta_name from groupUser")
  }

  def activityUser(spark : SparkSession): Unit = {
    var activityInfo = spark.sql("""val operate_user_id: Nothing = null
    val operate_user_name: Nothing = null
    val count: Nothing = null (1)
    val from: Nothing = null
    val where: Nothing = null execute_start_time >=
    ' 2019 - 02 - 01
    00:
    00:
    00
    ' val business_type: Nothing = 1
    val BY: Nothing = null operate_user_id
      operate_user_name""");
  }
}
