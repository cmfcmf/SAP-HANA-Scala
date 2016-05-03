package de.hpi.callcenterdashboard
// JDBC
import java.sql.{Connection, DriverManager, JDBCType}

class DatabaseConnection {
  val credentials = new Credentials()
  val driver = "com.sap.db.jdbc.Driver"
  val url = "jdbc:sap://" + credentials.hostname + ":" + credentials.port
  var connection = None : Option[Connection]

  def open(): Unit = {
    try {
      // make the connection
      Class.forName(driver)
      connection = Some(DriverManager.getConnection(url, credentials.username, credentials.password))

    } catch {
      // connection.get.close()
      case e: Throwable => e.printStackTrace()
    }
  }

  def close(): Unit = {
    connection.get.close()
  }

  def printCustomers(): Unit = {
    try {
      // create the statement, and run the select query
      val statement = connection.get.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM SAPQ92.KNA1_HPI LIMIT 100")
      while (resultSet.next()) {
        val knd_name = resultSet.getString("NAME")
        println(knd_name)
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }
}

