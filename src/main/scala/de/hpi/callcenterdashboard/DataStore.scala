package de.hpi.callcenterdashboard

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * DataStore for assignment 1. Requires you to pass a Credentials implementation.
  *
  * @param credentials The database credentials.
  */
class DataStore(credentials: CredentialsTrait) {
  private var connection = None: Option[Connection]
  private val tablePrefix = "SAPQ92"
  private val salesAccount = "0000893015"
  private val profitAccount = "0000792000"
  private val numOrders = 10
  private val numCustomers = 100
  private val years = List("2014", "2013")

  /**
    * Opens the database connection.
    */
  def open(): Unit = {
    try {
      // make the connection
      Class.forName("com.sap.db.jdbc.Driver")
      val url = "jdbc:sap://" + credentials.hostname + ":" + credentials.port
      connection = Some(DriverManager.getConnection(url, credentials.username, credentials.password))
    } catch {
      case e: Throwable => printError(e)
    }
  }

  /**
    * Checks whether or not the connection is opened.
    *
    * @return
    */
  def isOpened: Boolean = connection.exists(connection => !connection.isClosed)

  /**
    * Closes the database connection.
    */
  def close(): Unit = {
    connection.foreach(connection => {
      connection.close()
    })
  }

  /**
    * Prints an exception / error to the console.
    *
    * @param e The exception being thrown
    */
  private def printError(e: Throwable): Unit = {
    println("#####\n#####\nERROR during database connection:\n" + e.getLocalizedMessage + "\n#####\n#####")
  }

  /**
    * Returns a list of customers matching the given data. If a customer id is given, name and zip code are
    * ignored and vice versa.
    *
    * @param customerId Customer id.
    * @param name       Customer name
    * @param zip        Customer's zip code.
    * @return
    */
  def getCustomersBy(customerId: String, name: String, zip: String): List[Customer] = {
    if (customerId != "") {
      getCustomersById(customerId: String)
    } else if (zip != "" || name != "") {
      getCustomersByZipOrName(name: String, zip: String)
    } else {
      List.empty[Customer]
    }
  }

  /**
    * Get all customers matching the given id.
    *
    * @param customerId Customer id
    * @param fuzzy      Whether or not to use a fuzzy search or a regular LIKE %?% query.
    * @return
    */
  def getCustomersById(customerId: String, fuzzy: Boolean = false): List[Customer] = {
    var customers = List.empty[Customer]
    connection.foreach(connection => {
      var sql = ""
      if (fuzzy) {
        sql = s"SELECT SCORE() AS score, * FROM $tablePrefix.KNA1_HPI " +
          "WHERE CONTAINS(KUNDE, ?, FUZZY(0.5)) " +
          "ORDER BY score DESC " +
          s"LIMIT $numCustomers"
      } else {
        sql = s"SELECT * FROM $tablePrefix.KNA1_HPI " +
          "WHERE LOWER(KUNDE) LIKE LOWER('%' || ? || '%')" +
          s"LIMIT $numCustomers"
      }
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, customerId)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          customers = customers :+ new Customer(resultSet)
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })

    customers
  }

  /**
    * Get all customers by name and zip code.
    *
    * @param name Customer name
    * @param zip Customer zip code
    * @return
    */
  def getCustomersByZipOrName(name: String, zip: String): List[Customer] = {
    var customers = List.empty[Customer]
    connection.foreach(connection => {
      var sql = s"SELECT SCORE() AS score, * FROM $tablePrefix.KNA1_HPI WHERE "
      if (name != "") sql += "CONTAINS(NAME, ?, FUZZY(0.8))"
      if (zip != "") {
        if (name != "") sql += " AND "
        sql += "CONTAINS(PLZ, ?, FUZZY(0.9))"
      }
      sql += s"ORDER BY score DESC LIMIT $numCustomers"

      try {
        val preparedStatement = connection.prepareStatement(sql)
        if (name != "") preparedStatement.setString(1, name)
        if (zip != "") preparedStatement.setString(if (name != "") 2 else 1, zip)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          customers = customers :+ new Customer(resultSet)
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })

    customers
  }

  /**
    * Fetches a single customer by it's id.
    *
    * @param customerId The customer id.
    * @return
    */
  def getSingleCustomerById(customerId: String): Option[Customer] = {
    var customer = None: Option[Customer]

    connection.foreach(connection => {
      val sql = s"SELECT * FROM $tablePrefix.KNA1_HPI WHERE KUNDE = ?"
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, customerId)

        val resultSet = preparedStatement.executeQuery()
        if (resultSet.next()) {
          customer = Some(new Customer(resultSet))
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })

    customer
  }

  /**
    * Get orders of a given customer.
    *
    * @param customer The customer
    * @return
    */
  def getOrdersOf(customer: Customer): List[Order] = {
    var orders = List.empty[Order]
    connection.foreach(connection => {
      val sql = s"SELECT * FROM $tablePrefix.ACDOCA_HPI WHERE KUNDE = ?" +
        s"AND KONTO = $salesAccount ORDER BY BUCHUNGSDATUM DESC LIMIT $numOrders"
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, customer.customerId)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          orders = orders :+ new Order(resultSet)
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })

    orders
  }

  /**
    * Calculate sales and profit values by year for the given customer.
    *
    * @param customer A customer.
    * @return A list of tuples like (year, (salesAmount, currency), (profitAmount, currency))
    */
  def getSalesAndProfitOf(customer: Customer): List[(String, (BigDecimal, String), (BigDecimal, String))] = {
    var resultMap: Map[(String, String), (BigDecimal, String)] = Map()
    if (years.nonEmpty) {
      connection.foreach(connection => {
        // Create String with format (?,?,?,?) for PreparedStatement
        var yearString = "(?"
        for (i <- 1 until years.length) {
          yearString += ",?"
        }
        yearString += ") "

        val sql = "SELECT GESCHAFTSJAHR, KONTO, SUM(HAUS_BETRAG) as amount, HAUS_WAEHRUNG " +
          s"FROM $tablePrefix.ACDOCA_HPI " +
          "WHERE GESCHAFTSJAHR IN " + yearString +
          "AND KUNDE = ? " +
          s"AND KONTO IN ($profitAccount, $salesAccount) " +
          "GROUP BY GESCHAFTSJAHR, KONTO, HAUS_WAEHRUNG"

        try {
          val preparedStatement = connection.prepareStatement(sql)
          // Insert years into PreparedStatement
          for (i <- years.indices) {
            preparedStatement.setString(i + 1, years(i))
          }
          preparedStatement.setString(years.length + 1, customer.customerId)

          val resultSet = preparedStatement.executeQuery()
          while (resultSet.next()) {
            val year = resultSet.getString("GESCHAFTSJAHR")
            val account = resultSet.getString("KONTO")
            resultMap += ((year, account) -> (resultSet.getBigDecimal("amount"), resultSet.getString("HAUS_WAEHRUNG")))
          }
        } catch {
          case e: Throwable => printError(e)
        }
      })
    }

    for (year <- years) yield {
      val sales = resultMap.getOrElse((year, salesAccount), (BigDecimal("0.00"), "EUR"))
      val profit = resultMap.getOrElse((year, profitAccount), (BigDecimal("0.00"), "EUR"))
      (year, sales, (sales._1 + profit._1, profit._2))
    }
  }
}

