package de.hpi.callcenterdashboard.entity

import java.sql.ResultSet

class Product(result: ResultSet) {
  val id = result.getString("MATERIAL")
  val name = result.getString("TEXT")
  val salesSum = Money(result.getBigDecimal("AMOUNT"), result.getString("HAUS_WAEHRUNG"))
}
