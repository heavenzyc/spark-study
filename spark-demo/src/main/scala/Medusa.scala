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
    var all = user.join(dept, user("dep_id") === dept("id")).join(sta, user("jobs_id") === sta("id"))
    all.select("operate_user_name", "operate_user_id", "cnt", "dep_id", "dep_name", "jobs_id", "sta_name").show()
    all.createOrReplaceTempView("groupUser")
    spark.sql("select * from groupUser").show()
    spark.sql("insert into ana_result.medusa_group_user select operate_user_name, operate_user_id, cnt, dep_id, dep_name, jobs_id, sta_name from groupUser")
  }

  def activityUser(spark : SparkSession): Unit = {
    var activityInfo = spark.sql(
      """SELECT operate_user_id, operate_user_name, count(1) cnt
        |from dmall_medusa.push_activity
        |where execute_start_time>='2019-02-01 00:00:00'
        |and business_type=1
        |GROUP BY operate_user_id, operate_user_name
        |order by cnt desc""")
    activityInfo.count()
    var baseUser = spark.sql("select id, dep_id  from dmall_erp.erp_base_user where user_status=1")
    var dept = spark.sql("select id, dep_name from dmall_erp.erp_dep_info")
    var user = activityInfo.join(baseUser, activityInfo("operate_user_id") === baseUser("id"))
    var all = user.join(dept, user("dep_id") === dept("id"))
    all.createOrReplaceTempView("activityUser") // 创建临时视图
    // spark.catalog.dropTempView("activityInfo") // 删除临时视图
    spark.sql("insert into ana_result.medusa_activity_user select operate_user_id,operate_user_name, cnt, dep_id, dep_name from activityUser")
  }
}
