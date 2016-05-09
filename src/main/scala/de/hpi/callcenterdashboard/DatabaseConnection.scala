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

  def getCustomerBy(kdnr: String = "", name: String = "", plz: String = ""): List[Customer] = {
    try {
      if (kdnr != "") {
        val statement = "SELECT SCORE() AS score, * FROM SAPQ92.KNA1_HPI " +
          "WHERE CONTAINS(KUNDE, ?, FUZZY(0.8)) " +
          "ORDER BY score DESC " +
          "LIMIT 100"
        val preparedStatement = connection.get.prepareStatement(statement)
        preparedStatement.setString(1, kdnr)
        val resultSet = preparedStatement.executeQuery()
        var customers: List[Customer] = List.empty
        while (resultSet.next()) {
          val temp: List[Customer] = List(new Customer(resultSet))
          customers = List.concat(customers, temp)
        }
        return customers
      } else {
        if (plz != "" && name != "") {
          val sql = "SELECT SCORE() AS score, * FROM SAPQ92.KNA1_HPI " +
            "WHERE CONTAINS(NAME, ?, FUZZY(0.8)) AND CONTAINS(PLZ, ?, FUZZY(0.9)) " +
            "ORDER BY score DESC " +
            "LIMIT 100"
          val preparedStatement = connection.get.prepareStatement(sql)
          preparedStatement.setString(1, name)
          preparedStatement.setString(2, plz)
          val resultSet = preparedStatement.executeQuery()
          var customers: List[Customer] = List.empty
          while (resultSet.next()) {
            //val temp: List[Customer] = List(new Customer(resultSet))
            customers :+ new Customer(resultSet)
            //customers = List.concat(customers, temp)
          }
          return customers
        }
        else {
          return List.empty
        }
      }
    } catch {
        case e: Throwable => return List.empty
    }
  }

  def getSingleCustomerBy(customerId : String) : Customer = {
    val sql ="SELECT * FROM SAPQ92.KNA1_HPI WHERE KUNDE = ?"
    val preparedStatement = connection.get.prepareStatement(sql)
    preparedStatement.setString(1, customerId)
    val resultSet = preparedStatement.executeQuery()
    resultSet.next()
    return new Customer(resultSet)
  }

  def getOrderOf(customerId : String) : Order = {
    return new Order()
  }

  def getSalesOf(customerID: String, years: List[String]): Map[String, String] = {
    if (years.nonEmpty) {
      try {
        //create String with format (?,?,?,?) for PreparedStatement
        var yearString = "(?"
        for (i <- 1 until years.length) {
          yearString += ",?"
        }
        yearString += ")"

        val statement = "SELECT GESCHAFTSJAHR, SUM(HAUS_BETRAG) as betrag " +
          "FROM SAPQ92.ACDOCA_HPI " +
          "WHERE GESCHAFTSJAHR IN " + yearString +
          " AND KUNDE = ? " +
          "AND KONTO = '0000893015' " +
          "GROUP BY GESCHAFTSJAHR"
        val preparedStatement = connection.get.prepareStatement(statement)

        //insert years into PreparedStatement
        for (i <- years.indices) {
          preparedStatement.setString(i + 1, years(i).toString)
        }
        preparedStatement.setString(years.length + 1, customerID)
        val resultSet = preparedStatement.executeQuery()

        var umsatzMap: Map[String, String] = Map()
        while(resultSet.next()) {
          val year = resultSet.getString("GESCHAFTSJAHR")
          umsatzMap += (year -> resultSet.getBigDecimal("betrag").toString)
        }
        return umsatzMap
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
    return Map()
  }

}

