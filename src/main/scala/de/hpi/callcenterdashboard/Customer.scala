package de.hpi.callcenterdashboard

import java.sql.ResultSet

class Customer(result: ResultSet) {
  val customerNumber = result.getString("KUNDE")
  val country = result.getString("LAND")
  val name = result.getString("NAME")
  val town = result.getString("ORT")
  val zip = result.getString("PLZ")
  val region = result.getString("REGION")
  val street = result.getString("STRASSE")
  val business = result.getString("BRANCHE")
  val group = result.getString("KUNDENGRUPPE")
}
