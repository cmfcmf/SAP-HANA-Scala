package de.hpi.callcenterdashboard
// JDBC
import java.sql.{Connection, DriverManager, JDBCType}

class DatabaseConnection {
  val credentials = new Credentials()
  val driver = "com.sap.db.jdbc.Driver"
  val url = "jdbc:sap://" + credentials.hostname + ":" + credentials.port
  var con: Option[Connection] = None
  try {
    // make the connection
    Class.forName(driver)
    con = Some(DriverManager.getConnection(url, credentials.username, credentials.password))
    con.map(connection => {
      // create the statement, and run the select query
      val statement = connection.createStatement()
      //val resultSet = statement.executeQuery("SELECT RBUKRS, BUDssfAT, KSL, RKCUR FROM ACDOCA LIMIT 100")
      val resultSet = statement.executeQuery("SELECT * FROM SAPQ92.KNA1_HPI LIMIT 100")
      while (resultSet.next()) {
        val knd_name = resultSet.getString("NAME")
        println(knd_name)
      }
    })
  } catch {
    case e: Throwable => e.printStackTrace
  } finally {
    con.map(_.close())
  }
}

