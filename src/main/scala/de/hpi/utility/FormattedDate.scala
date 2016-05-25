package de.hpi.utility

import java.text.SimpleDateFormat
import java.util.Date

object DateFormatter {

  def today() : FormattedDate = {
    val today = new Date()
    new FormattedDate(new SimpleDateFormat("yyyyMMdd").format(today))
  }
}

class FormattedDate(date: String, inputFormat: String = "yyyyMMdd") {
  private val rawDate = date
  private val asDate = new SimpleDateFormat(inputFormat).parse(rawDate)

  def as_yyyyMMdd(separator: String = "/") : String = {
    new SimpleDateFormat(s"yyyy${separator}MM${separator}dd").format(asDate)
  }

  def as_ddMMyyyy(separator: String = ".") : String = {
    new SimpleDateFormat(s"dd${separator}MM${separator}yyyy").format(asDate)
  }

  def unformatted : String = {
    rawDate
  }

  override def toString : String = {
    as_yyyyMMdd()
  }
}