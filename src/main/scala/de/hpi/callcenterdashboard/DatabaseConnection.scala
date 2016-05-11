package de.hpi.callcenterdashboard
// JDBC
import java.sql.{Connection, DriverManager, PreparedStatement}

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
          println(s"search using zip $plz and name $name")
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
            val temp: List[Customer] = List(new Customer(resultSet))
            customers = List.concat(customers, temp)
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

  def getOrdersOf(customerId : String) : List[Order] = {
    val statement =   "SELECT * FROM SAPQ92.ACDOCA_HPI WHERE KUNDE = ?" +
                      "AND KONTO = 0000893015 ORDER BY BUCHUNGSDATUM DESC LIMIT 10"
    val preparedStatement = connection.get.prepareStatement(statement)
    preparedStatement.setString(1, customerId)
    val resultSet = preparedStatement.executeQuery()
    var orders: List[Order] = List.empty
    while (resultSet.next()) {
      orders = orders :+ new Order(resultSet)
    }
    return orders
  }

  def executeStatementOn(statement: PreparedStatement, account: String, index: Int): Map[String, BigDecimal] = {
    statement.setString(index, account)
    val resultSet = statement.executeQuery()
    var resultMap: Map[String, BigDecimal] = Map()
    while(resultSet.next()) {
      val year = resultSet.getString("GESCHAFTSJAHR")
      resultMap += (year -> resultSet.getBigDecimal("betrag"))
    }
    println(resultMap)
    return resultMap
  }

  def generatePreparedStatement(customerID: String, years: List[String]): PreparedStatement = {
    //create String with format (?,?,?,?) for PreparedStatement
    var yearString = "(?"
    for (i <- 1 until years.length) {
      yearString += ",?"
    }
    yearString += ") "

    val statement = "SELECT GESCHAFTSJAHR, SUM(HAUS_BETRAG) as betrag " +
      "FROM SAPQ92.ACDOCA_HPI " +
      "WHERE GESCHAFTSJAHR IN " + yearString +
      "AND KUNDE = ? " +
      "AND KONTO = ? " +
      "GROUP BY GESCHAFTSJAHR"
    val preparedStatement = connection.get.prepareStatement(statement)

    //insert years into PreparedStatement
    for (i <- years.indices) {
      preparedStatement.setString(i + 1, years(i).toString)
    }
    preparedStatement.setString(years.length + 1, customerID)
    return preparedStatement
  }

  def getSalesAndProfitOf(customerID: String, years: List[String]): List[(String, String, String)] = {
    val salesAccount = "0000893015"
    val costsAccount = "0000792000"

    if (years.nonEmpty) {
      try {
        val preparedStatement = generatePreparedStatement(customerID, years)
        val costsMap = executeStatementOn(preparedStatement, costsAccount, years.length + 2)
        val salesMap = executeStatementOn(preparedStatement, salesAccount, years.length + 2)

        return for (year <- years) yield {
          val costs: BigDecimal = costsMap.getOrElse(year, 0)
          val sales: BigDecimal = salesMap.getOrElse(year, 0)
          (year, sales.toString, (sales + costs).toString)
        }
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
    return List(("", "", ""))
  }
}

