package de.hpi.callcenterdashboard

import de.hpi.callcenterdashboard.entity._
import de.hpi.callcenterdashboard.utility._
import java.sql.{Connection, DriverManager}

/**
  * DataStore for assignment 1 and 2. Requires you to pass a Credentials implementation.
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
  private val mandant = 800
  private val language = "'E'"
  private val calendarId = "'01'"

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
    println("#####\n#####\nERROR during database connection:\n" + e.getMessage + "\n#####\n#####")
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

  private val selectFromKNA1SQL = s"""
          SELECT kunde.*, land.NAME as LAND_TEXT, region.BEZEI AS REGION_TEXT
          FROM $tablePrefix.KNA1_HPI kunde
          JOIN $tablePrefix.T005T_HPI AS land ON (kunde.LAND = land.LAND AND land.SPRACHE = $language)
          LEFT JOIN $tablePrefix.T005U as region ON (region.MANDT = $mandant AND kunde.REGION = region.BLAND AND kunde.LAND = region.LAND1 AND region.SPRAS = $language)
    """

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
      var sql = selectFromKNA1SQL
      if (fuzzy) {
        sql += s"""
          WHERE CONTAINS(kunde.KUNDE, ?, FUZZY(0.5))
          ORDER BY SCORE() DESC
          LIMIT $numCustomers
          """
      } else {
        sql += s"""
          WHERE LOWER(kunde.KUNDE) LIKE LOWER('%' || ? || '%')
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
              $selectFromKNA1SQL
              WHERE
                (? = '' OR CONTAINS(kunde.NAME, ?, FUZZY(0.8)))
                AND
                (? = ''  OR CONTAINS(kunde.PLZ, ?, FUZZY(0.9)))
              ORDER BY SCORE() DESC
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
      val sql = s"""
              SELECT kunde.*, land.NAME as LAND_TEXT, region.BEZEI AS REGION_TEXT
              FROM $tablePrefix.KNA1_HPI kunde
              JOIN $tablePrefix.T005T_HPI AS land ON (kunde.LAND = land.LAND AND land.SPRACHE = $language)
              LEFT JOIN $tablePrefix.T005U as region ON (region.MANDT = $mandant AND kunde.REGION = region.BLAND AND kunde.LAND = region.LAND1 AND region.SPRAS = $language)

              WHERE KUNDE = ?
        """
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
              SELECT
                bestellung.*,
                material.MATERIALNUMMER, material.TEXT AS MATERIAL_TEXT,
                werk.WERK AS WERK, werk.NAME2 AS WERK_NAME, werk.STRASSE AS WERK_STRASSE, werk.ZIPCODE AS WERK_PLZ, werk.CITY AS WERK_STADT, werk.REGION AS WERK_REGION, werk.LAND AS WERK_LAND
              FROM $tablePrefix.ACDOCA_HPI bestellung
              JOIN $tablePrefix.T001W_HPI werk ON (werk.WERK = bestellung.WERK)
              JOIN $tablePrefix.MAKT_HPI material ON (material.MATERIALNUMMER = bestellung.MATERIAL)
              WHERE
                bestellung.KUNDE = ?
                AND
                bestellung.KONTO = $salesAccount
              ORDER BY bestellung.BUCHUNGSDATUM DESC
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
    * Returns all orders of the given customer which aren't yet fully paid until the given date.
    *
    * @param customer The customer to check.
    * @param date The date until which the orders would have to be paid.
    * @return
    */
  def getOutstandingOrdersOfCustomerUpTo(customer: Customer, date: FormattedDate): List[Order] = {
    var orders = List.empty[Order]
    connection.foreach(connection => {
      val sql = s"""
            SELECT *, bestell_summe
            FROM (
              SELECT
                (SUM(HAUS_BETRAG) OVER(ORDER BY BUCHUNGSDATUM DESC, BELEGNUMMER DESC) - (HAUS_BETRAG)) * -1 AS bestell_summe,
                bestellung.*,
                material.MATERIALNUMMER, material.TEXT AS MATERIAL_TEXT,
                werk.WERK AS WERK, werk.NAME2 AS WERK_NAME, werk.STRASSE AS WERK_STRASSE, werk.ZIPCODE AS WERK_PLZ, werk.CITY AS WERK_STADT, werk.REGION AS WERK_REGION, werk.LAND AS WERK_LAND
              FROM $tablePrefix.ACDOCA_HPI bestellung
              JOIN $tablePrefix.T001W_HPI werk ON (werk.WERK = bestellung.WERK)
              JOIN $tablePrefix.MAKT_HPI material ON (material.MATERIALNUMMER = bestellung.MATERIAL)

              WHERE bestellung.KUNDE = ?
              AND bestellung.BUCHUNGSDATUM <= ?
              AND bestellung.KONTO = $costsAccount
            )
            WHERE bestell_summe < (
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

  def getProductSalesPercent(customer: Customer, startDate: FormattedDate, endDate: FormattedDate): List[(Product, Float, Money)] = {
    var products = List.empty[(Product, Float, Money)]
    connection.foreach(connection => {
      // @todo We currently return all products ever sold to that customer.
      val totalAmountQuery = s"""
        SELECT SUM(HAUS_BETRAG) AS GESAMMT_UMSATZ
        FROM $tablePrefix.ACDOCA_HPI
        WHERE BUCHUNGSDATUM BETWEEN ? AND ?
          AND KUNDE = ?
          AND KONTO = $salesAccount
        """
      val sql = s"""
        SELECT
          SUM(bestellung.HAUS_BETRAG) AS UMSATZ, bestellung.HAUS_WAEHRUNG,
          (SUM(bestellung.HAUS_BETRAG) / GESAMMT_UMSATZ) * 100 AS UMSATZANTEIL,
          GESAMMT_UMSATZ,
          bestellung.MATERIAL AS MATERIAL,
          material.TEXT AS MATERIAL_TEXT
        FROM
          $tablePrefix.ACDOCA_HPI bestellung,
          ($totalAmountQuery),
          $tablePrefix.MAKT_HPI material
        WHERE bestellung.BUCHUNGSDATUM BETWEEN ? AND ?
          AND bestellung.KUNDE = ?
          AND bestellung.KONTO = $salesAccount
          AND material.MATERIALNUMMER = bestellung.MATERIAL
        GROUP BY bestellung.KUNDE, bestellung.MATERIAL, material.TEXT, bestellung.HAUS_WAEHRUNG, GESAMMT_UMSATZ
        ORDER BY SUM(bestellung.HAUS_BETRAG) DESC
        """

      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, startDate.unformatted)
        preparedStatement.setString(2, endDate.unformatted)
        preparedStatement.setString(3, customer.customerId)
        preparedStatement.setString(4, startDate.unformatted)
        preparedStatement.setString(5, endDate.unformatted)
        preparedStatement.setString(6, customer.customerId)
        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          products = products :+ (
            new Product(resultSet),
            resultSet.getFloat("UMSATZANTEIL"),
            Money(resultSet.getBigDecimal("UMSATZ"), resultSet.getString("HAUS_WAEHRUNG"))
            )
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    products
  }

  /**
    * Calculates the average amount of working days it takes the customer to pay.
    *
    * @param customer The customer.
    * @return
    */
  def getAveragePaymentTimeOfCustomer(customer : Customer): Int = {
    var averagePaymentTime = 0
    connection.foreach(connection => {
      val sql = s"""
              SELECT
                  ROUND(AVG(WORKDAYS_BETWEEN($calendarId, A.BUCHUNGSDATUM, B.BUCHUNGSDATUM, '$tablePrefix')), 0) AS ZAHLUNGSDAUER
                  FROM $tablePrefix.ACDOCA_HPI AS A
                  JOIN $tablePrefix.ACDOCA_HPI AS B ON (
                      A.BELEGNUMMER = B.BELEGNUMMER AND A.KONTO = $costsAccount AND B.KONTO = $salesAccount
                  )
              WHERE A.KUNDE = ?
              GROUP BY A.KUNDE
        """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, customer.customerId)

        val resultSet = preparedStatement.executeQuery()
        if (resultSet.next()) {
          averagePaymentTime = resultSet.getInt("ZAHLUNGSDAUER")
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    averagePaymentTime
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
  def getSalesForRegionsOfCountry(countryCode : String, startDate: FormattedDate, endDate: FormattedDate):
    List[(String, String, Money)] = {
    var sales = List.empty[(String, String, Money)]
    connection.foreach(connection => {
      val sql =
        s"""
            SELECT SUM(HAUS_BETRAG) as sales, HAUS_WAEHRUNG, t.BEZEI AS region_name
            FROM $tablePrefix.ACDOCA_HPI AS a
              JOIN $tablePrefix.KNA1_HPI AS k ON a.KUNDE = k.KUNDE
              JOIN $tablePrefix.T005U as t ON (t.MANDT = $mandant AND REGION = t.BLAND AND LAND = t.LAND1 AND t.SPRAS = 'E')
            WHERE
              KONTO = $salesAccount
              AND BUCHUNGSDATUM BETWEEN ? AND ?
              AND LAND = ?
              AND REGION <> ''
            GROUP BY t.BEZEI, HAUS_WAEHRUNG
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, startDate.unformatted)
        preparedStatement.setString(2, endDate.unformatted)
        preparedStatement.setString(3, countryCode)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          sales = sales :+ (
            countryCode,
            resultSet.getString("region_name"),
            Money(resultSet.getBigDecimal("sales"), resultSet.getString("HAUS_WAEHRUNG"))
            )
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    sales
  }

  def getWorldWideSales(startDate: FormattedDate, endDate: FormattedDate): List[(String, Money, String)] = {
    var sales = List.empty[(String, Money, String)]
    connection.foreach(connection => {
      val sql =
        s"""
            SELECT SUM(HAUS_BETRAG) as sales, HAUS_WAEHRUNG, k.LAND AS LAENDERKUERZEL, b.NAME AS LANDNAME
            FROM $tablePrefix.ACDOCA_HPI AS a
              JOIN $tablePrefix.KNA1_HPI AS k ON a.KUNDE = k.KUNDE
              JOIN $tablePrefix.T005T_HPI AS b ON k.LAND = b.LAND
            WHERE
              KONTO = $salesAccount
              AND BUCHUNGSDATUM BETWEEN ? AND ?
              AND SPRACHE = 'E'
            GROUP BY k.LAND, b.NAME, HAUS_WAEHRUNG
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, startDate.unformatted)
        preparedStatement.setString(2, endDate.unformatted)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          sales = sales :+ (
            resultSet.getString("LAENDERKUERZEL"),
            Money(resultSet.getBigDecimal("sales"), resultSet.getString("HAUS_WAEHRUNG")),
            resultSet.getString("LANDNAME")
            )
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    sales
  }
}
