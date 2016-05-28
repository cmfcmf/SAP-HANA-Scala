package de.hpi.callcenterdashboard.entity

import java.sql.ResultSet

class Product(result: ResultSet) {
  val id = result.getString("MATERIAL")
  val name = result.getString("MATERIAL_TEXT")
}
