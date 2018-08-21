import java.sql.{Connection, DriverManager, Timestamp}

object ScalaMysql {
  def main(args: Array[String]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val username = "root"
    val password = "heaven"

    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, password)
    val  stat = connection.createStatement()
    val resultSet = stat.executeQuery("select * from rizhi limit 10")
    var list : List[String] = List()
    while (resultSet.next()) {
      list = list:+(resultSet.getString("phone"))
    }
    list.foreach(println _)
    connection.close()
  }
}
