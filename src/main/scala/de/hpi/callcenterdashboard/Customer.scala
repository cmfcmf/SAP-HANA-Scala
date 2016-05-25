package de.hpi.callcenterdashboard

import java.sql.ResultSet

class Customer(result: ResultSet, avgPaymentTime: (Customer) => Int) {

  val customerId = result.getString("KUNDE")
  val country = result.getString("LAND")
  val name = result.getString("NAME")
  val town = result.getString("ORT")
  val zip = result.getString("PLZ")
  val region = if (result.getString("REGION").isEmpty) false else result.getString("REGION")
  val street = result.getString("STRASSE")
  val business = if (result.getString("BRANCHE").isEmpty) false else result.getString("BRANCHE")
  val group = if (result.getString("KUNDENGRUPPE").isEmpty) false else result.getString("KUNDENGRUPPE")
  lazy val averagePaymentTime = {avgPaymentTime(this)}
}
