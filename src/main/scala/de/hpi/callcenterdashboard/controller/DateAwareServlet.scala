package de.hpi.callcenterdashboard.controller

import de.hpi.callcenterdashboard.utility.{DateFormatter, FormattedDate}
import org.scalatra.scalate.ScalateSupport
import org.scalatra.{ScalatraServlet, SessionSupport}

trait DateAwareServlet extends ScalatraServlet with ScalateSupport with SessionSupport {
  before() {
    params.get("startDate").foreach(date => {
      if (date.nonEmpty)
        session.setAttribute("startDate", new FormattedDate(date, "yyyy-MM-dd"))
      else
        session.removeAttribute("startDate")
    })
    params.get("endDate").foreach(date => {
      if (date.nonEmpty)
        session.setAttribute("endDate", new FormattedDate(date, "yyyy-MM-dd"))
      else
        session.removeAttribute("endDate")
    })

    templateAttributes("startDate") = session.getAttribute("startDate")
    templateAttributes("endDate") = session.getAttribute("endDate")
  }

  def startDate: FormattedDate = {
    session.getAttribute("startDate") match {
      case date: FormattedDate => date
      case _ => DateFormatter.past
    }
  }

  def endDate: FormattedDate = {
    session.getAttribute("endDate") match {
      case date: FormattedDate => date
      case _ => DateFormatter.today
    }
  }
}
