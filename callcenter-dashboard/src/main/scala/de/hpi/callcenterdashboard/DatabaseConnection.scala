/*
// JDBC
import java.sql.{Connection, DriverManager, JDBCType}

val driver = "com.mysql.jdbc.Driver"
val url = "jdbc:mysql://hpiscala.ccvhkpvpzoym.eu-central-1.rds.amazonaws.com"
val username = "hpi_scala"
val password = "..."
var con: Option[Connection] = None
try {
  // make the connection
  Class.forName(driver)
  con = Some(DriverManager.getConnection(url, username, password))
  con.map( connection => {
    // create the statement, and run the select query
    val statement = connection.createStatement()
    //val resultSet = statement.executeQuery("SELECT RBUKRS, BUDssfAT, KSL, RKCUR FROM ACDOCA LIMIT 100")
    val resultSet = statement.executeQuery("SELECT RBUKRS, BUDAT, KSL, RKCUR FROM SAPISP.ACDOCA LIMIT 100")
    while (resultSet.next()) {
      val buchungskreis = resultSet.getString("RBUKRS")
      val buchungsdatum = resultSet.getString("BUDAT")
      val profit = resultSet.getInt("KSL")
      val unit = resultSet.getString("RKCUR")
      //resultSet.getBlob("pskhlsf")
      println(s"$buchungskreis $buchungsdatum $profit$unit")
    }
  })
} catch {
  case e: Throwable => e.printStackTrace
} finally {
  con.map(_.close())
}
// Aufgabe: use SUM, WHERE, ORDERBY, GROUPBY, LIMIT, OFFSET
// play around: slick, scalikejdbc
// hint: scaling issue (ACDOCA) -> platform
//
*/
