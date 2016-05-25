package de.hpi.utility

import java.sql.SQLData
import java.text.SimpleDateFormat
import java.util.Date

object DateFormatter {

  def today : FormattedDate = {
    val today = new Date()
    new FormattedDate(new SimpleDateFormat("yyyyMMdd").format(today))
  }
}

class FormattedDate(date: String, inputFormat: String = "yyyyMMdd") {
  val asDate = new SimpleDateFormat(inputFormat).parse(date)
  val asSQLDate = new java.sql.Date(asDate.getTime())

  def as_yyyyMMdd(separator: String = "/") : String = {
    new SimpleDateFormat(s"yyyy${separator}MM${separator}dd").format(asDate)
  }

  def as_ddMMyyyy(separator: String = ".") : String = {
    new SimpleDateFormat(s"dd${separator}MM${separator}yyyy").format(asDate)
  }

  def unformatted : String = {
    new SimpleDateFormat("yyyyMMdd").format(asDate)
  }

  override def toString : String = {
    as_yyyyMMdd()
  }
}