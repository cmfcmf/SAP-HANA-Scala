package de.hpi.callcenterdashboard

import java.math.BigDecimal
import java.sql.ResultSet
import java.text.SimpleDateFormat

class Order(result: ResultSet) {
  val accountingArea = result.getString("BUCHUNGSKREIS")
  val accountingYear = result.getString("GESCHAFTSJAHR")
  val referenceNumber = result.getString("BELEGNUMMER")
  val debitAndCredit = result.getString("SOLL_HABEN_KEN")
  val account = result.getString("KONTO")
  val houseAmount = result.getBigDecimal("HAUS_BETRAG")
  val houseCurrency = result.getString("HAUS_WAEHRUNG")
  val transactionAmount = result.getBigDecimal("TRANSAKTIONS_BETRAG")
  val transactionCurrency = result.getString("TRANSAKTIONS_WAEHRUNG")
  val customer = result.getString("KUNDE")
  val workPiece = result.getString("WERK")
  val material = result.getString("MATERIAL")
  val ft = new SimpleDateFormat ("yyyyMMdd")
  val tempDate = ft.parse(result.getString("BUCHUNGSDATUM"))
  val ft2 = new SimpleDateFormat("yyyy/MM/dd")
  val bookingDate = ft2.format(tempDate)
}
