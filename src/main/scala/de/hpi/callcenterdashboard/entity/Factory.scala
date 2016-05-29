package de.hpi.callcenterdashboard.entity

import java.sql.ResultSet

class Factory(result: ResultSet) {
  val id = result.getString("WERK")

  val name = result.getString("WERK_NAME")

  val street = result.getString("WERK_STRASSE")
  val zip = result.getString("WERK_PLZ")
  val town = result.getString("WERK_STADT")
  val region = if (result.getString("WERK_REGION").isEmpty) false else result.getString("WERK_REGION")
  val country = result.getString("WERK_LAND")

  def address = s"$country, $zip $town, $street"
}
