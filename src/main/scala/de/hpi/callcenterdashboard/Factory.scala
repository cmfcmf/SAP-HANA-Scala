package de.hpi.callcenterdashboard

import java.sql.ResultSet

class Factory(result: ResultSet) {
  val id = result.getString("WERK")

  val name = result.getString("NAME2")

  val street = result.getString("STRASSE")
  val zip = result.getString("ZIPCODE")
  val town = result.getString("CITY")
  val region = if (result.getString("REGION").isEmpty) false else result.getString("REGION")
  val country = result.getString("LAND")

  def address = s"$country, $zip $town, $street"
}
