package de.hpi.callcenterdashboard
// JDBC
import java.sql.{Connection, DriverManager}

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
      if (kdnr != "") {
        val statement = "SELECT * FROM SAPQ92.KNA1_HPI WHERE CONTAINS(KUNDE, ?, FUZZY(0.8)) LIMIT 100"
        val preparedStatement = connection.get.prepareStatement(statement)
        preparedStatement.setString(1, kdnr)
        println (preparedStatement)
        val resultSet = preparedStatement.executeQuery()
        printCustomerResults(resultSet)
      } else {
        if (plz != "" && name != "") {
          val statement = "SELECT * FROM SAPQ92.KNA1_HPI " +
            "WHERE CONTAINS(NAME, ?, FUZZY(0.8)) AND CONTAINS(PLZ, ?, FUZZY(0.9)) LIMIT 100"
          val preparedStatement = connection.get.prepareStatement(statement)
          preparedStatement.setString(1, name)
          preparedStatement.setString(2, plz)
          val resultSet = preparedStatement.executeQuery()
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

