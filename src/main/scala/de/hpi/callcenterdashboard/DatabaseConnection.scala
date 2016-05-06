package de.hpi.callcenterdashboard
// JDBC
import java.sql.{Connection, DriverManager, JDBCType}

import com.sap.db.jdbc.trace.ResultSet

class DatabaseConnection {
  val credentials = new Credentials()
  val driver = "com.sap.db.jdbc.Driver"
  val url = "jdbc:sap://" + credentials.hostname + ":" + credentials.port
  var connection = None: Option[Connection]

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
  def printCustomerResults (resultSet : java.sql.ResultSet): Unit = {
    while (resultSet.next()) {
      println(resultSet.getString("NAME"))
      println(resultSet.getString("STRASSE") + ", " + resultSet.getString("PLZ") + " " + resultSet.getString("ORT") + ", " + resultSet.getString("LAND"))
      println(resultSet.getString("BRANCHE") + " " + resultSet.getString("KUNDENGRUPPE"))
    }
  }
  def printCustomersBy(kdnr: String = "", name: String = "", plz: String = ""): Unit = {
    try {
      // create the statement, and run the select query
      val statement = connection.get.createStatement()
      if (kdnr != "") {
        val resultSet = statement.executeQuery("SELECT * FROM SAPQ92.KNA1_HPI WHERE LOWER(KUNDE) LIKE LOWER('" + kdnr + "') LIMIT 100")
        printCustomerResults(resultSet)
      } else {
        if (plz != "" && name != "") {
          val resultSet = statement.executeQuery("SELECT * FROM SAPQ92.KNA1_HPI " +
            "WHERE LOWER(NAME) LIKE LOWER('" + name + "')AND PLZ LIKE '" + plz + "' LIMIT 100")
          printCustomerResults(resultSet)
        }
        else {
          println("Please insert either customer's ID or the customer's name and zip code.")
        }
      }
    } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
}

