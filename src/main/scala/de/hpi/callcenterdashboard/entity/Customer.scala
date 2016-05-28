package de.hpi.callcenterdashboard.entity

import java.sql.ResultSet

class Customer(result: ResultSet, avgPaymentTime: (Customer) => Int) {
  val customerId = result.getString("KUNDE")
  val country = result.getString("LAND_TEXT")
  val name = result.getString("NAME")
  val town = result.getString("ORT")
  val zip = result.getString("PLZ")
  val region = if (Option(result.getString("REGION_TEXT")).getOrElse("").isEmpty) false else result.getString("REGION_TEXT")
  val street = result.getString("STRASSE")
  val business = if (Option(result.getString("BRANCHE")).getOrElse("").isEmpty) false else result.getString("BRANCHE")
  val group = if (result.getString("KUNDENGRUPPE").isEmpty) false else result.getString("KUNDENGRUPPE")
  lazy val averagePaymentTime = {avgPaymentTime(this)}
}
