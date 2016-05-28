package de.hpi.callcenterdashboard.entity

import java.sql.ResultSet

import de.hpi.callcenterdashboard.utility._

import de.hpi.callcenterdashboard.utility._

class Order(result: ResultSet) {
  val accountingArea = result.getString("BUCHUNGSKREIS")
  val accountingYear = result.getString("GESCHAFTSJAHR")
  val referenceNumber = result.getString("BELEGNUMMER")
  val account = result.getString("KONTO")
  val houseMoney = Money(
    result.getBigDecimal("HAUS_BETRAG").abs(),
    result.getString("HAUS_WAEHRUNG")
  )
  val transactionMoney = Money(
    result.getBigDecimal("TRANSAKTIONS_BETRAG").abs(),
    result.getString("TRANSAKTIONS_WAEHRUNG")
  )
  val customer = result.getString("KUNDE")
  val factory = new Factory(result)
  val workPiece = new Workpiece(result)
  val bookingDate = new FormattedDate(result.getString("BUCHUNGSDATUM"))
}
