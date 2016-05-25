package de.hpi.callcenterdashboard

import org.scalatra.ScalatraServlet
import org.scalatra.scalate.ScalateSupport

trait DateAwareServlet extends ScalatraServlet with ScalateSupport {
  before() {
    if (params.getOrElse('startDate, "").nonEmpty) session.setAttribute("startDate", params('startDate))
    if (params.getOrElse('endDate, "").nonEmpty) session.setAttribute("endDate", params('endDate))

    templateAttributes("startDate") = session.getAttribute("startDate")
    templateAttributes("endDate") = session.getAttribute("endDate")
    templateAttributes("isGetRequest") = request.getMethod == "GET"
  }
}
