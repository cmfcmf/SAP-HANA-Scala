package de.hpi.callcenterdashboard

import de.hpi.utility.FormattedDate
import org.scalatra.{ScalatraServlet, SessionSupport}
import org.scalatra.scalate.ScalateSupport

trait DateAwareServlet extends ScalatraServlet with ScalateSupport with SessionSupport {
  before() {
    if (params.getOrElse('startDate, "").nonEmpty)
      session.setAttribute("startDate", new FormattedDate(params('startDate), "yyyy-MM-dd"))
    if (params.getOrElse('endDate, "").nonEmpty)
      session.setAttribute("endDate", new FormattedDate(params('endDate), "yyyy-MM-dd"))

    templateAttributes("startDate") = session.getAttribute("startDate")
    templateAttributes("endDate") = session.getAttribute("endDate")
    templateAttributes("isGetRequest") = request.getMethod == "GET"
  }
}
