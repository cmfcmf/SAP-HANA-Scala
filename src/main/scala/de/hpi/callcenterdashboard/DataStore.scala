package de.hpi.callcenterdashboard

import de.hpi.callcenterdashboard.entity._
import de.hpi.callcenterdashboard.utility._
import java.sql.{Connection, DriverManager}

/**
  * DataStore for assignment 1. Requires you to pass a Credentials implementation.
  *
  * @param credentials The database credentials.
  */
class DataStore(credentials: CredentialsTrait) {
  private var connection = None: Option[Connection]
  private val tablePrefix = "SAPQ92"
  private val salesAccount = "0000893015"
  private val costsAccount = "0000792000"
  private val numOrders = 10
  private val numCustomers = 100
  private val years = List("2014", "2013")
  private val houseCurrency = "EUR"

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
        sql = s"""
          SELECT SCORE() AS score, * FROM $tablePrefix.KNA1_HPI
          WHERE CONTAINS(KUNDE, ?, FUZZY(0.5))
          ORDER BY score DESC
          LIMIT $numCustomers
          """
      } else {
        sql = s"""
          SELECT * FROM $tablePrefix.KNA1_HPI
          WHERE LOWER(KUNDE) LIKE LOWER('%' || ? || '%')
          LIMIT $numCustomers
          """
      }
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, customerId)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          customers = customers :+ new Customer(resultSet, getAveragePaymentTimeOfCustomer)
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
      val sql = s"""
              SELECT SCORE() AS score, * FROM $tablePrefix.KNA1_HPI
              WHERE
                (? = '' OR CONTAINS(NAME, ?, FUZZY(0.8)))
                AND
                (? = ''  OR CONTAINS(PLZ, ?, FUZZY(0.9)))
              ORDER BY score DESC
              LIMIT $numCustomers
              """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, name)
        preparedStatement.setString(2, name)
        preparedStatement.setString(3, zip)
        preparedStatement.setString(4, zip)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          customers = customers :+ new Customer(resultSet, getAveragePaymentTimeOfCustomer)
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
          customer = Some(new Customer(resultSet, getAveragePaymentTimeOfCustomer))
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
      val sql = s"""
              SELECT *
              FROM $tablePrefix.ACDOCA_HPI a
              LEFT JOIN $tablePrefix.T001W_HPI t ON (t.WERK = a.WERK)
              LEFT JOIN $tablePrefix.MAKT_HPI m ON (m.MATERIALNUMMER = a.MATERIAL)
              WHERE
                KUNDE = ?
                AND
                KONTO = $salesAccount
              ORDER BY BUCHUNGSDATUM DESC
              LIMIT $numOrders
              """
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
    * @return A list of tuples like (year: String, sales: Money, profit: Money)
    */
  def getSalesAndProfitOf(customer: Customer): List[(String, Money, Money)] = {
    var resultMap: Map[(String, String), Money] = Map()
    if (years.nonEmpty) {
      connection.foreach(connection => {
        // Create String with format (?,?,?,?) for PreparedStatement
        var yearString = "(?"
        for (i <- 1 until years.length) {
          yearString += ",?"
        }
        yearString += ") "

        val sql = s"""
          SELECT GESCHAFTSJAHR, KONTO, SUM(HAUS_BETRAG) as amount, HAUS_WAEHRUNG
          FROM $tablePrefix.ACDOCA_HPI
          WHERE
            GESCHAFTSJAHR IN $yearString
            AND KUNDE = ?
            AND KONTO IN ($costsAccount, $salesAccount)
          GROUP BY GESCHAFTSJAHR, KONTO, HAUS_WAEHRUNG
          """

        try {
          val preparedStatement = connection.prepareStatement(sql)
          // Insert years into PreparedStatement
          for (i <- years.indices) {
            preparedStatement.setString(i + 1, years(i))
          }
          preparedStatement.setString(years.length + 1, customer.customerId)

          val resultSet = preparedStatement.executeQuery()
          while (resultSet.next()) {
            resultMap += (resultSet.getString("GESCHAFTSJAHR"), resultSet.getString("KONTO")) -> Money(
              resultSet.getBigDecimal("amount"),
              resultSet.getString("HAUS_WAEHRUNG")
            )
          }
        } catch {
          case e: Throwable => printError(e)
        }
      })
    }

    for (year <- years) yield {
      val sales = resultMap.getOrElse((year, salesAccount), Money(BigDecimal("0.00"), houseCurrency))
      val costs = resultMap.getOrElse((year, costsAccount), Money(BigDecimal("0.00"), houseCurrency))
      (year, sales, sales + costs)
    }
  }

  def getProductSalesPercent(customerId: String, startDate: FormattedDate, endDate: FormattedDate): List[(Product, Float)] = {
    var products = List.empty[(Product, Float)]
    connection.foreach(connection => {
      val totalAmountQuery = s"""
        SELECT SUM(HAUS_BETRAG) AS TOTAL_AMOUNT
        FROM $tablePrefix.ACDOCA_HPI
        WHERE BUCHUNGSDATUM >= ?
        AND BUCHUNGSDATUM <= ?
        AND KUNDE = ?
        AND KONTO = $salesAccount
        """
      val sql = s"""
        SELECT MATERIAL, TEXT, SUM(HAUS_BETRAG) AS AMOUNT, (SUM(HAUS_BETRAG) / TOTAL_AMOUNT) AS PERCENTAGE, HAUS_WAEHRUNG
        FROM $tablePrefix.ACDOCA_HPI, $tablePrefix.MAKT_HPI, ($totalAmountQuery)
        WHERE BUCHUNGSDATUM >= ?
        AND BUCHUNGSDATUM <= ?
        AND KUNDE = ?
        AND KONTO = $salesAccount
        AND $tablePrefix.ACDOCA_HPI.MATERIAL = $tablePrefix.MAKT_HPI.MATERIALNUMMER
        GROUP BY KUNDE, MATERIAL, TEXT, HAUS_WAEHRUNG, TOTAL_AMOUNT
        ORDER BY SUM(HAUS_BETRAG) DESC
        """

      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, startDate.unformatted)
        preparedStatement.setString(2, endDate.unformatted)
        preparedStatement.setString(3, customerId)
        preparedStatement.setString(4, startDate.unformatted)
        preparedStatement.setString(5, endDate.unformatted)
        preparedStatement.setString(6, customerId)
        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          products = products :+ (new Product(resultSet), resultSet.getFloat("PERCENTAGE") * 100)
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    products
  }

  def getProductHitlist(numProducts: Int = 0, startDate: FormattedDate, endDate: FormattedDate): List[Product] = {
    var products = List.empty[Product]
    connection.foreach(connection => {
      val sql =
        s"""
            SELECT MATERIAL, TEXT, SUM(HAUS_BETRAG) AS AMOUNT, HAUS_WAEHRUNG
            FROM $tablePrefix.ACDOCA_HPI, $tablePrefix.MAKT_HPI
            WHERE
              BUCHUNGSDATUM BETWEEN ? AND ?
              AND KONTO = $salesAccount
              AND $tablePrefix.ACDOCA_HPI.MATERIAL = $tablePrefix.MAKT_HPI.MATERIALNUMMER
            GROUP BY MATERIAL, TEXT, HAUS_WAEHRUNG
            ORDER BY SUM(HAUS_BETRAG) DESC
            LIMIT ?
          """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, startDate.unformatted)
        preparedStatement.setString(2, endDate.unformatted)
        preparedStatement.setInt(3, numProducts)
        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          products = products :+ new Product(resultSet)
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    products
  }

  def getOutstandingOrdersOfCustomerUpTo(customer: Customer, date: FormattedDate): List[Order] = {
    var orders = List.empty[Order]
    connection.foreach(connection => {
      val sql = s"""
            SELECT *, amount
            FROM (
              SELECT *, (SUM(HAUS_BETRAG) OVER(ORDER BY BUCHUNGSDATUM DESC, BELEGNUMMER DESC) - (HAUS_BETRAG)) * -1 AS AMOUNT
              FROM $tablePrefix.ACDOCA_HPI a
              LEFT JOIN $tablePrefix.T001W_HPI t ON (t.WERK = a.WERK)
              LEFT JOIN $tablePrefix.MAKT_HPI m ON (m.MATERIALNUMMER = a.MATERIAL)
              WHERE KUNDE = ?
              AND BUCHUNGSDATUM <= ?
              AND KONTO = $costsAccount
            )
            WHERE AMOUNT < (
              SELECT SUM(HAUS_BETRAG) * (-1) as amount
              FROM $tablePrefix.ACDOCA_HPI
              WHERE KUNDE = ?
              AND BUCHUNGSDATUM <= ?
              AND KONTO IN ($costsAccount, $salesAccount)
            )
        """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, customer.customerId)
        preparedStatement.setString(2, date.unformatted)
        preparedStatement.setString(3, customer.customerId)
        preparedStatement.setString(4, date.unformatted)

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

  def getAveragePaymentTimeOfCustomer(customer : Customer): Int = {
    var averagePaymentTime = 0
    connection.foreach(connection => {
      val sql = s"""
              SELECT AVG(paymentDiff) AS avgPaymentTime
              FROM(
                SELECT
                  A.KUNDE AS KUNDE,
                  WORKDAYS_BETWEEN('01', A.BUCHUNGSDATUM, B.BUCHUNGSDATUM, '$tablePrefix') AS paymentDiff
                FROM $tablePrefix.ACDOCA_HPI AS A
                JOIN $tablePrefix.ACDOCA_HPI AS B ON (
                  A.BELEGNUMMER = B.BELEGNUMMER AND A.KONTO = $costsAccount AND B.KONTO = $salesAccount
                )
              )
              WHERE KUNDE = ?
              GROUP BY KUNDE
        """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, customer.customerId)

        val resultSet = preparedStatement.executeQuery()
        if (resultSet.next()) {
          averagePaymentTime = Math.round(resultSet.getFloat("avgPaymentTime"))
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    averagePaymentTime
  }

  def getSalesOfCountryOrRegion(country: String, region: String, startDate: FormattedDate, endDate: FormattedDate) : Money = {
    var salesOfCountryOrRegion = new Money(0, houseCurrency)
    connection.foreach(connection => {
      val sql =
        s"""
            SELECT SUM(HAUS_BETRAG) AS sales, HAUS_WAEHRUNG
            FROM $tablePrefix.ACDOCA_HPI AS A JOIN $tablePrefix.KNA1_HPI AS B ON A.KUNDE = B.KUNDE
            WHERE
              ( ? = '' OR CONTAINS(REGION, ?, FUZZY(0.8)))
              AND
              ( ? = ''  OR CONTAINS(LAND, ?, FUZZY(0.8)))
            AND KONTO = $salesAccount
            AND BUCHUNGSDATUM BETWEEN ? AND ?
            GROUP BY LAND, HAUS_WAEHRUNG
        """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, region)
        preparedStatement.setString(2, region)
        preparedStatement.setString(3, country)
        preparedStatement.setString(4, country)
        preparedStatement.setString(5, startDate.unformatted)
        preparedStatement.setString(6, endDate.unformatted)

        val resultSet = preparedStatement.executeQuery()
        if (resultSet.next()) {
          salesOfCountryOrRegion = Money(
            resultSet.getBigDecimal("sales"),
            resultSet.getString("HAUS_WAEHRUNG"))
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    println(salesOfCountryOrRegion)
    salesOfCountryOrRegion
  }

  /**
    * Given a start and end date, return a list of tuples of (Country, SalesSum) for each country
    * where we sold something. The SalesSum is the sum of all sales for that country.
    *
    * @param startDate The start date.
    * @param endDate   The end date.
    * @return
    */
  def getWorldWideSales(startDate: FormattedDate, endDate: FormattedDate): List[(String, Money)] = {
    var sales = List.empty[(String, Money)]
    connection.foreach(connection => {
      val sql =
        s"""
            SELECT SUM(HAUS_BETRAG) as sales, HAUS_WAEHRUNG, LAND
            FROM $tablePrefix.ACDOCA_HPI AS a JOIN $tablePrefix.KNA1_HPI AS k ON a.KUNDE = k.KUNDE
            WHERE
              KONTO = $salesAccount
              AND BUCHUNGSDATUM BETWEEN ? AND ?
            GROUP BY LAND, HAUS_WAEHRUNG
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, startDate.unformatted)
        preparedStatement.setString(2, endDate.unformatted)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          sales = sales :+ (
            resultSet.getString("LAND"),
            Money(resultSet.getBigDecimal("sales"), resultSet.getString("HAUS_WAEHRUNG"))
            )
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })

    sales
  }
}
