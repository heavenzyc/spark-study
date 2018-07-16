import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSQLDemo")
    sparkConf.setMaster("local")
    val spark = SparkSession.builder().appName("SparkSQLDemo").config(sparkConf).getOrCreate()
    System.setProperty("hadoop.home.dir", "D:\\winutils\\bin")
    runJDBCDataSource(spark)
//    loadDataSourceFromeJson(spark)
//    loadDataSourceFromeParquet(spark)
//    runFromRDD(spark)
    spark.stop()
  }

  private def runJDBCDataSource(spark: SparkSession): Unit = {
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/fruit-erp?user=root&password=heaven")
      .option("dbtable", "erp_base_user") //必须写表名
      .load()
//    jdbcDF.select("user_name", "email", "mobile").write.format("parquet").save("src/main/resources/sec_users")
    //jdbcDF.select("username", "name", "telephone").write.format("json").save("src/main/resources/sec_users")

    //存储成为一张虚表user_abel
    var df = jdbcDF.select("user_name", "email", "mobile")//.write.mode("overwrite").saveAsTable("user_abel")
    val jdbcSQl = spark.sql("select * from user_abel where name like '王%' ")
    jdbcSQl.show()
    jdbcSQl.write.format("json").save("./out/resulted")
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
