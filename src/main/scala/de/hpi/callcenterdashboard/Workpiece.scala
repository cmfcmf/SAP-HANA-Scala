package de.hpi.callcenterdashboard

import java.sql.ResultSet

class Workpiece(result: ResultSet) {
  val id = result.getString("MATERIALNUMMER")
  val name = result.getString("TEXT")
}
