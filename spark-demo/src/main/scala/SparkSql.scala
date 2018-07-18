import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object SparkSql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSQLDemo")
//    sparkConf.setMaster("local")
    val spark = SparkSession.builder().appName("SparkSQLDemo").config(sparkConf).getOrCreate()
//    System.setProperty("hadoop.home.dir", "D:\\winutils")
    runJDBCDataSource(spark)
//    loadDataSourceFromeJson(spark)
//    loadDataSourceFromeParquet(spark)
//    runFromRDD(spark)
    spark.stop()
  }

  private def runJDBCDataSource(spark: SparkSession): Unit = {
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.8.207:13306/dmall_erp?user=wumart&password=!QAZxsw2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "erp_base_user") //必须写表名
      .load().select("id", "badge_no", "gender")
    val jdbcDF1 = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.8.207:13306/dmall_erp?user=wumart&password=!QAZxsw2")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "erp_user_role") //必须写表名
      .load().select("user_id")
//    jdbcDF.select("user_name", "email", "mobile").write.format("parquet").save("src/main/resources/sec_users")
    //jdbcDF.select("username", "name", "telephone").write.format("json").save("src/main/resources/sec_users")
    var joinDF = jdbcDF1.join(jdbcDF , jdbcDF1("user_id" ) === jdbcDF( "id"))
    joinDF.show(10)
    var group = joinDF.groupBy(jdbcDF("badge_no"))
    group.count().orderBy("count")
    var rows = joinDF.groupBy(jdbcDF("gender")).count().collect()
    for (row <- rows)
      println(row)

//    group.count().show()
    //存储成为一张虚表user_abel
    /*var df = jdbcDF.select("user_name", "email", "mobile")
    df.show();
    var rows = df.collect();
    println(rows)
    df.write.mode("overwrite").saveAsTable("user_abel")
    val jdbcSQl = spark.sql("select * from user_abel where user_name like '小%' ")
    jdbcSQl.show()*/
//    jdbcSQl.write.format("json").save("./out/resulted")
  }

  private def loadDataSourceFromeJson(spark: SparkSession): Unit = {
    //load 方法是加载parquet 列式存储的数据
    // val jsonDF=spark.read.load("src/main/resources/sec_users/user.json")
    val jsonDF = spark.read.json("src/main/resources/user.json")

    jsonDF.printSchema()
    //创建临时视图
    jsonDF.createOrReplaceTempView("user")
    val namesDF = spark.sql("SELECT name FROM user WHERE name like '王%'")
    import spark.implicits._
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    jsonDF.select("name").write.format("json").save("./out/resultedJSON")
  }

  private def loadDataSourceFromeParquet(spark: SparkSession): Unit = {

    val parquetDF = spark.read.load("src/main/resources/user.parquet")
    parquetDF.createOrReplaceTempView("user")
    val namesDF = spark.sql("SELECT name FROM user WHERE id > 1 ")
    namesDF.show()

    parquetDF.select("name").write.format("parquet").save("./out/resultedParquet")
  }

  private def runFromRDD(spark: SparkSession): Unit = {
    val otherPeopleRDD = spark.sparkContext.makeRDD(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleRDD)
    otherPeople.show()
  }
}
