package de.hpi.callcenterdashboard

import org.scalatra.{ScalatraParams, SessionSupport}
import org.scalatra.scalate.ScalateSupport
import de.hpi.utility._

class CompanyServlet extends DataStoreAwareServlet with ScalateSupport with DateAwareServlet {
  get("/statistics") {
    contentType = "text/html"
    val salesHitlist = dataStore.getProductHitlist(
      10,
      session.getAttribute("startDate").asInstanceOf[FormattedDate],
      session.getAttribute("endDate").asInstanceOf[FormattedDate]
    )
    layoutTemplate("/company-statistics", "products" -> salesHitlist)
  }
}