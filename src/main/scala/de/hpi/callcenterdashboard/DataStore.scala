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
  private val years = List("2015", "2014", "2013")
  private val houseCurrency = "EUR"
  private val mandant = 800
  private val language = "'E'"
  private val calendarId = "'01'"
  private val bukrs = "'F010'"

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
                acdoca.MSL AS MENGE,
                material.MATERIALNUMMER, material.TEXT AS MATERIAL_TEXT,
                werk.WERK AS WERK, werk.NAME2 AS WERK_NAME, werk.STRASSE AS WERK_STRASSE, werk.ZIPCODE AS WERK_PLZ, werk.CITY AS WERK_STADT, werk.REGION AS WERK_REGION, werk.LAND AS WERK_LAND
              FROM $tablePrefix.ACDOCA_HPI bestellung
              JOIN $tablePrefix.T001W_HPI werk ON (werk.WERK = bestellung.WERK)
              JOIN $tablePrefix.MAKT_HPI material ON (material.MATERIALNUMMER = bestellung.MATERIAL)
              JOIN $tablePrefix.ACDOCA acdoca ON (acdoca.RCLNT = 800 AND acdoca.RBUKRS = 'F010' AND acdoca.GJAHR = bestellung.GESCHAFTSJAHR AND acdoca.BELNR = bestellung.BELEGNUMMER AND acdoca.RACCT = bestellung.KONTO)
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
            SELECT *, BESTELL_SUMME
            FROM (
              SELECT
                (SUM(HAUS_BETRAG) OVER(ORDER BY BUCHUNGSDATUM DESC, BELEGNUMMER DESC) - (HAUS_BETRAG)) * -1 AS BESTELL_SUMME,
                bestellung.*,
                acdoca.MSL AS MENGE,
                material.MATERIALNUMMER, material.TEXT AS MATERIAL_TEXT,
                werk.WERK AS WERK, werk.NAME2 AS WERK_NAME, werk.STRASSE AS WERK_STRASSE, werk.ZIPCODE AS WERK_PLZ, werk.CITY AS WERK_STADT, werk.REGION AS WERK_REGION, werk.LAND AS WERK_LAND
              FROM $tablePrefix.ACDOCA_HPI bestellung
              JOIN $tablePrefix.T001W_HPI werk ON (werk.WERK = bestellung.WERK)
              JOIN $tablePrefix.MAKT_HPI material ON (material.MATERIALNUMMER = bestellung.MATERIAL)
              JOIN $tablePrefix.ACDOCA acdoca ON (acdoca.RCLNT = 800 AND acdoca.RBUKRS = 'F010' AND acdoca.GJAHR = bestellung.GESCHAFTSJAHR AND acdoca.BELNR = bestellung.BELEGNUMMER AND acdoca.RACCT = bestellung.KONTO)

              WHERE bestellung.KUNDE = ?
              AND bestellung.BUCHUNGSDATUM <= ?
              AND bestellung.KONTO = $costsAccount
            )
            WHERE BESTELL_SUMME < (
              SELECT SUM(HAUS_BETRAG) * (-1) as OFFENER_BETRAG
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

  /**
    * For a given customer and start and end date, calculate how much of the sales the sold products make up.
    *
    * @param customer The customer.
    * @param startDate The start date.
    * @param endDate The end date.
    * @return Triples of the form (Product, Percentage, Absolute amount)
    */
  def getProductSalesPercent(customer: Customer, startDate: FormattedDate, endDate: FormattedDate): List[(Product, Float, Money)] = {
    var products = List.empty[(Product, Float, Money)]
    connection.foreach(connection => {
      // @todo We currently return all products ever sold to that customer.
      val totalAmountQuery = s"""
        SELECT SUM(HAUS_BETRAG) AS GESAMT_UMSATZ
        FROM $tablePrefix.ACDOCA_HPI
        WHERE BUCHUNGSDATUM BETWEEN ? AND ?
          AND KUNDE = ?
          AND KONTO = $salesAccount
        """
      val sql = s"""
        SELECT
          SUM(bestellung.HAUS_BETRAG) AS UMSATZ, bestellung.HAUS_WAEHRUNG AS WAEHRUNG,
          (SUM(bestellung.HAUS_BETRAG) / GESAMT_UMSATZ) * 100 AS UMSATZANTEIL,
          GESAMT_UMSATZ,
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
        GROUP BY bestellung.KUNDE, bestellung.MATERIAL, material.TEXT, bestellung.HAUS_WAEHRUNG, GESAMT_UMSATZ
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

  /**
    * Creates a list of products which were sold best in the given timefframe.
    *
    * @param numProducts The number of products to return.
    * @param filter The filter to apply.
    * @return A list of touples containing the product and the sales sum.
    */
  def getCashCowProducts(numProducts: Int = 0, filter: Filter): List[(Product, Money)] = {
    var products = List.empty[(Product, Money)]
    connection.foreach(connection => {
      val sql =
        s"""
            SELECT
              SUM(HAUS_BETRAG) AS SUMME, bestellung.HAUS_WAEHRUNG AS WAEHRUNG,
              bestellung.MATERIAL AS MATERIAL, material.TEXT AS MATERIAL_TEXT
            FROM $tablePrefix.ACDOCA_HPI bestellung
            JOIN $tablePrefix.KNA1_HPI kunde ON (kunde.KUNDE = bestellung.KUNDE)
            JOIN $tablePrefix.MAKT_HPI material ON (material.MATERIALNUMMER = bestellung.MATERIAL)
            JOIN $tablePrefix.MARA_HPI material_info_int ON (material_info_int.MATERIALNUMMER = bestellung.MATERIAL)
            WHERE
              bestellung.BUCHUNGSDATUM BETWEEN ? AND ?
              AND bestellung.KONTO = $salesAccount
              AND (? = '' OR kunde.LAND = ?)
              AND (? = '' OR kunde.REGION = ?)
              AND (? = '' OR bestellung.WERK = ?)
              AND (? = '' OR material_info_int.MATERIALART = ?)
              AND EXISTS(
                SELECT * FROM $tablePrefix.MVKE_HPI material_info_ext
                WHERE
                  material_info_ext.MATERIALNUMMER = bestellung.MATERIAL
                  AND material_info_ext.PRODUKTHIERARCHIE LIKE ? || '%'
                  AND (? = '' OR material_info_ext.VETRIEBSORGANISATION = ?)
              )
              AND (? = '' OR bestellung.material = ?)
            GROUP BY bestellung.MATERIAL, material.TEXT, bestellung.HAUS_WAEHRUNG
            ORDER BY SUM(bestellung.HAUS_BETRAG) DESC
            LIMIT ?
          """

      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, filter.startDate.unformatted)
        preparedStatement.setString(2, filter.endDate.unformatted)
        preparedStatement.setString(3, filter.countryId)
        preparedStatement.setString(4, filter.countryId)
        preparedStatement.setString(5, filter.regionId)
        preparedStatement.setString(6, filter.regionId)
        preparedStatement.setString(7, filter.factoryId)
        preparedStatement.setString(8, filter.factoryId)
        preparedStatement.setString(9, filter.materialType)
        preparedStatement.setString(10, filter.materialType)
        preparedStatement.setString(11, filter.productHierarchyVal)
        preparedStatement.setString(12, filter.salesOrganization)
        preparedStatement.setString(13, filter.salesOrganization)
        preparedStatement.setString(14, filter.productId)
        preparedStatement.setString(15, filter.productId)
        preparedStatement.setInt(16, numProducts)
        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          products = products :+ (
            new Product(resultSet),
            Money(resultSet.getBigDecimal("SUMME"), resultSet.getString("HAUS_WAEHRUNG"))
            )
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    products
  }

  /**
    * Get the world wide sales sum in a given date range.
    *
    * @param filter The filter to apply.
    * @return Triples of short country code, sales sum, country name.
    */
  def getWorldWideSales(filter: Filter): List[(Country, Money, List[(Region, Money)])] = {
    var sales = List.empty[(Country, Money, List[(Region, Money)])]
    connection.foreach(connection => {
      val regionSql =
        s"""
           SELECT
             SUM(HAUS_BETRAG) AS REGION_SUMME,
             region.BEZEI     AS REGION_NAME,
             kunde.LAND       AS REGION_LAND,
             kunde.REGION     AS REGION_ID
           FROM $tablePrefix.ACDOCA_HPI AS bestellung
           JOIN $tablePrefix.KNA1_HPI AS kunde ON bestellung.KUNDE = kunde.KUNDE
           JOIN $tablePrefix.T005U AS region ON (
             kunde.REGION = region.BLAND
             AND
             kunde.LAND = region.LAND1
             AND region.SPRAS = 'E' AND region.MANDT = $mandant
           )
           JOIN $tablePrefix.MARA_HPI material_info_int ON (material_info_int.MATERIALNUMMER = bestellung.MATERIAL)
           WHERE
             bestellung.KONTO = $salesAccount
             AND bestellung.BUCHUNGSDATUM BETWEEN ? AND ?
             AND kunde.REGION <> ''
             AND (? = '' OR kunde.REGION = ?)
             AND (? = '' OR bestellung.WERK = ?)
             AND (? = '' OR material_info_int.MATERIALART = ?)
             AND EXISTS(
              SELECT * FROM $tablePrefix.MVKE_HPI material_info_ext
              WHERE
                material_info_ext.MATERIALNUMMER = bestellung.MATERIAL
                AND material_info_ext.PRODUKTHIERARCHIE LIKE ? || '%'
                AND (? = '' OR material_info_ext.VETRIEBSORGANISATION = ?)
             )
             AND (? = '' OR bestellung.material = ?)
           GROUP BY region.BEZEI, kunde.LAND, kunde.REGION
         """

      val sql =
        s"""
           SELECT
             SUM(bestellung.HAUS_BETRAG) AS SUMME,
             bestellung.HAUS_WAEHRUNG,
             kunde.LAND                  AS LAND,
             land.NAME                   AS LAND_NAME,
             REGION_SUMME,
             REGION_NAME,
             REGION_ID
           FROM $tablePrefix.ACDOCA_HPI AS bestellung
           JOIN $tablePrefix.KNA1_HPI AS kunde ON bestellung.KUNDE = kunde.KUNDE
           JOIN $tablePrefix.T005T_HPI AS land ON (kunde.LAND = land.LAND AND land.SPRACHE = $language)
           JOIN $tablePrefix.MAKT_HPI material ON (material.MATERIALNUMMER = bestellung.MATERIAL)
           JOIN $tablePrefix.MARA_HPI material_info_int ON (material_info_int.MATERIALNUMMER = bestellung.MATERIAL)
           LEFT JOIN ($regionSql) regionen ON (regionen.REGION_LAND = land.LAND)
           WHERE
             bestellung.BUCHUNGSDATUM BETWEEN ? AND ?
             AND bestellung.KONTO = $salesAccount
             AND (? = '' OR kunde.LAND = ?)
             AND (? = '' OR kunde.REGION = ?)
             AND (? = '' OR bestellung.WERK = ?)
             AND (? = '' OR material_info_int.MATERIALART = ?)
             AND EXISTS(
               SELECT * FROM $tablePrefix.MVKE_HPI material_info_ext
               WHERE
                 material_info_ext.MATERIALNUMMER = bestellung.MATERIAL
                 AND material_info_ext.PRODUKTHIERARCHIE LIKE ? || '%'
                 AND (? = '' OR material_info_ext.VETRIEBSORGANISATION = ?)
             )
             AND (? = '' OR bestellung.material = ?)
           GROUP BY kunde.LAND, land.NAME, bestellung.HAUS_WAEHRUNG, REGION_SUMME, REGION_NAME, REGION_ID
           ORDER BY land.NAME ASC, REGION_NAME ASC
         """

      try {
        val preparedStatement = connection.prepareStatement(sql)
        var i = 0
        preparedStatement.setString(1, filter.startDate.unformatted)
        preparedStatement.setString(2, filter.endDate.unformatted)
        preparedStatement.setString(3, filter.regionId)
        preparedStatement.setString(4, filter.regionId)
        preparedStatement.setString(5, filter.factoryId)
        preparedStatement.setString(6, filter.factoryId)
        preparedStatement.setString(7, filter.materialType)
        preparedStatement.setString(8, filter.materialType)
        preparedStatement.setString(9, filter.productHierarchyVal)
        preparedStatement.setString(10, filter.salesOrganization)
        preparedStatement.setString(11, filter.salesOrganization)
        preparedStatement.setString(12, filter.productId)
        preparedStatement.setString(13, filter.productId)

        preparedStatement.setString(14, filter.startDate.unformatted)
        preparedStatement.setString(15, filter.endDate.unformatted)
        preparedStatement.setString(16, filter.countryId)
        preparedStatement.setString(17, filter.countryId)
        preparedStatement.setString(18, filter.regionId)
        preparedStatement.setString(19, filter.regionId)
        preparedStatement.setString(20, filter.factoryId)
        preparedStatement.setString(21, filter.factoryId)
        preparedStatement.setString(22, filter.materialType)
        preparedStatement.setString(23, filter.materialType)
        preparedStatement.setString(24, filter.productHierarchyVal)
        preparedStatement.setString(25, filter.salesOrganization)
        preparedStatement.setString(26, filter.salesOrganization)
        preparedStatement.setString(27, filter.productId)
        preparedStatement.setString(28, filter.productId)

        val resultSet = preparedStatement.executeQuery()
        var currentCountryId = ""
        var regionalSales = List.empty[(Region, Money)]
        while (resultSet.next()) {
          // The following code is one of the ugliest things I've ever writte :-(
          if (resultSet.getString("LAND") != currentCountryId) {
            sales = sales :+(
              Country(resultSet.getString("LAND"), resultSet.getString("LAND_NAME")),
              Money(resultSet.getBigDecimal("SUMME"), resultSet.getString("HAUS_WAEHRUNG")),
              List.empty[(Region, Money)]
              )
            regionalSales = List.empty[(Region, Money)]
          }
          if (resultSet.getString("REGION_ID") != null) {
            regionalSales = regionalSales :+ (
              Region(resultSet.getString("REGION_ID"), resultSet.getString("REGION_NAME")),
              Money(resultSet.getBigDecimal("REGION_SUMME"), resultSet.getString("HAUS_WAEHRUNG"))
              )
            if (sales.nonEmpty) {
              val old = sales.last
              sales = sales.dropRight(1)
              sales = sales :+ (old._1, old._2, regionalSales)
            }
          }
          currentCountryId = resultSet.getString("LAND")
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    sales
  }

  /**
    * Get a list of all the factories.
    *
    * @return
    */
  def getFactories: List[Factory] = {
    var factories = List.empty[Factory]
    connection.foreach(connection => {
      val sql =
        s"""
           SELECT werk.WERK AS WERK, werk.NAME2 AS WERK_NAME, werk.STRASSE AS WERK_STRASSE, werk.ZIPCODE AS WERK_PLZ, werk.CITY AS WERK_STADT, werk.REGION AS WERK_REGION, werk.LAND AS WERK_LAND
           FROM $tablePrefix.T001W_HPI werk
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          factories = factories :+ new Factory(resultSet)
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    factories
  }

  def getCountries: List[Country] = {
    var countries = List.empty[Country]
    connection.foreach(connection => {
      val sql =
        s"""
           SELECT land.LAND AS LAND, land.NAME as LAND_NAME
           FROM $tablePrefix.T005T_HPI land
           WHERE land.SPRACHE = $language
           ORDER BY land.NAME ASC
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          countries = countries :+ new Country(resultSet.getString("LAND"), resultSet.getString("LAND_NAME"))
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    countries
  }

  def getRegionsForCountry(country: String): List[Region] = {
    var regions = List.empty[Region]
    connection.foreach(connection => {
      val sql =
        s"""
           SELECT region.BLAND AS ID, region.BEZEI AS NAME
           FROM $tablePrefix.T005U as region

           WHERE region.MANDT = $mandant AND region.SPRAS = $language AND region.LAND1 = ?
           ORDER BY NAME ASC
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, country)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          regions = regions :+ new Region(resultSet.getString("ID"), resultSet.getString("NAME"))
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    regions
  }

  def getMaterialTypes: List[(String, String)] = {
    var materialTypes = List.empty[(String, String)]
    connection.foreach(connection => {
      val sql =
        s"""
           SELECT DISTINCT material_typ.MTART AS ID, material_typ.MTBEZ AS NAME
           FROM $tablePrefix.T134T material_typ
           JOIN $tablePrefix.MARA_HPI as material ON (material.MATERIALART = material_typ.MTART)

           WHERE material_typ.SPRAS = $language
            AND material_typ.MANDT = $mandant
           ORDER BY material_typ.MTBEZ ASC
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          materialTypes = materialTypes :+ (resultSet.getString("ID"), resultSet.getString("NAME"))
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    materialTypes
  }

  def getSalesOrganizations: List[(String, String)] = {
    var salesOrganizsations = List.empty[(String, String)]
    connection.foreach(connection => {
      val sql =
        s"""
           SELECT tvkot.VKORG AS ID, tvkot.VTEXT AS NAME
           FROM $tablePrefix.TVKOT tvkot
           JOIN $tablePrefix.TVKO tvko ON (tvko.MANDT = tvkot.MANDT AND tvko.VKORG = tvkot.VKORG AND tvko.BUKRS = $bukrs)
           WHERE SPRAS = $language AND tvkot.MANDT = $mandant
           ORDER BY NAME ASC
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          salesOrganizsations = salesOrganizsations :+ (resultSet.getString("ID"), resultSet.getString("NAME"))
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    salesOrganizsations
  }

  def getProductHierarchy: List[(String, String, Int)] = {
    var productHierarchy = List.empty[(String, String, Int)]
    connection.foreach(connection => {
      val sql =
        s"""
           SELECT hierarchie.PRODH AS ID, hierarchie.STUFE AS STUFE, hierarchie_text.TEXT AS NAME
           FROM $tablePrefix.T179 hierarchie
           JOIN $tablePrefix.T179T_HPI as hierarchie_text ON (hierarchie.PRODH = hierarchie_text.PRODUKTHIERARCHIE)

           WHERE hierarchie.MANDT = $mandant
           ORDER BY ID ASC
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)

        val resultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          productHierarchy = productHierarchy :+ (resultSet.getString("ID"), resultSet.getString("NAME"), resultSet.getInt("STUFE"))
        }
      } catch {
        case e: Throwable => printError(e)
      }
    })
    productHierarchy
  }

  def getProducts: List[Product] = {
    var products = List.empty[Product]
    connection.foreach(connection => {
      val sql =
        s"""
           SELECT material_text.MATERIALNUMMER AS MATERIAL, material_text.TEXT AS MATERIAL_TEXT
           FROM $tablePrefix.MAKT_HPI material_text
           JOIN $tablePrefix.MVKE_HPI material_info ON (material_text.MATERIALNUMMER = material_info.MATERIALNUMMER)
           WHERE material_info.VETRIEBSORGANISATION IN (
             SELECT VKORG
             FROM $tablePrefix.TVKO tvko
             WHERE tvko.MANDT = 800 AND tvko.BUKRS = $bukrs
           )
           ORDER BY MATERIAL ASC
         """
      try {
        val preparedStatement = connection.prepareStatement(sql)

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
}
