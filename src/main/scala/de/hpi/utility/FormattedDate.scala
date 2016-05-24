package de.hpi.utility

import java.text.SimpleDateFormat
import java.util.Date

object FormattedDate {

  implicit class StringFormatter(val date: String) {
    def as_yyyyMMdd(separator: String = "/", inputFormat: String = "yyyyMMdd") = {
      var result = today_yyyyMMdd()

      if (date.length > 0) {
        val formatter = new SimpleDateFormat(inputFormat).parse(date)
        result = new SimpleDateFormat(s"yyyy${separator}MM${separator}dd").format(formatter)
      }
    }
  }

  def today_yyyyMMdd(separator: String = "/"): String = {
    val today : Date = new Date()
    new SimpleDateFormat(s"yyyy${separator}MM${separator}dd").format(today)
  }
}