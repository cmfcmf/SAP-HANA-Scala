package de.hpi.utility

import java.text.SimpleDateFormat
import java.util.Date

object DateFormatter {
  def today: FormattedDate = {
    val today = new Date()
    new FormattedDate(new SimpleDateFormat("yyyyMMdd").format(today))
  }
  def past: FormattedDate = {
    new FormattedDate("19900101")
  }
}

class FormattedDate(date: String, inputFormat: String = "yyyyMMdd") {
  val asDate = new SimpleDateFormat(inputFormat).parse(date)

  def as_yyyyMMdd(separator: String = "/") : String = {
    new SimpleDateFormat(s"yyyy${separator}MM${separator}dd").format(asDate)
  }

  def as_ddMMyyyy(separator: String = ".") : String = {
    new SimpleDateFormat(s"dd${separator}MM${separator}yyyy").format(asDate)
  }

  def unformatted : String = {
    new SimpleDateFormat("yyyyMMdd").format(asDate)
  }
  def forDatePicker: String = as_yyyyMMdd("-")

  override def toString : String = {
    as_yyyyMMdd()
  }
}