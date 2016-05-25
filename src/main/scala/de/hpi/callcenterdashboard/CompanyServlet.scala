package de.hpi.callcenterdashboard

import org.scalatra.{ScalatraParams, SessionSupport}
import org.scalatra.scalate.ScalateSupport

class CompanyServlet extends DataStoreAwareServlet with ScalateSupport {
  get("/statistics") {
    contentType = "text/html"
    val salesHitlist = dataStore.getProductHitlist(10, "20140101", "20150101")
    layoutTemplate("/company-statistics", "products" -> salesHitlist)
  }
}