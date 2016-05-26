package de.hpi.callcenterdashboard.entity

import java.sql.ResultSet

import de.hpi.callcenterdashboard.utility._

class Order(result: ResultSet) {
  val accountingArea = result.getString("BUCHUNGSKREIS")
  val accountingYear = result.getString("GESCHAFTSJAHR")
  val referenceNumber = result.getString("BELEGNUMMER")
  val debitAndCredit = result.getString("SOLL_HABEN_KEN")
  val account = result.getString("KONTO")
  val houseMoney = Money(
    result.getBigDecimal("HAUS_BETRAG"),
    result.getString("HAUS_WAEHRUNG"))
  val transactionMoney = Money(
    result.getBigDecimal("TRANSAKTIONS_BETRAG"),
    result.getString("TRANSAKTIONS_WAEHRUNG"))
  val customer = result.getString("KUNDE")
  val workPiece = result.getString("WERK")
  val material = result.getString("MATERIAL")
  val bookingDate = new FormattedDate(result.getString("BUCHUNGSDATUM"))
}
