import java.sql.{DriverManager, ResultSet}

/**
  * Created by heaven.zyc on 2018/7/15.
  */
object SparkMysql {
  def main(args: Array[String]): Unit = {
    conn();
  }

  def conn(): Seq[String] = {
    val user = "root"
    val password = "heaven"
    val host = "localhost"
    val database = "fruit-erp"
    val conn_str = "jdbc:mysql://" + host + ":3306/" + database + "?user=" + user + "&password=" + password
    //classOf[com.mysql.jdbc.Driver]
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    val conn = DriverManager.getConnection(conn_str)
    var setName = Seq("")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query，查询用户表 sec_user 是我的用户表，有name属性。
      val rs = statement.executeQuery("select * from erp_base_user")
      // Iterate Over ResultSet

      while (rs.next) {
        // 返回行号
        // println(rs.getRow)
        val name = rs.getString("user_name")
        setName = setName :+ name
      }
      println(s"setName: $setName")
      return setName
    }
    finally {
      conn.close
    }
  }
}
