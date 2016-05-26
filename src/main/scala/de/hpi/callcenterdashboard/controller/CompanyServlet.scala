package de.hpi.callcenterdashboard.controller

import org.scalatra.scalate.ScalateSupport

class CompanyServlet extends DataStoreAwareServlet with ScalateSupport with DateAwareServlet {
  get("/statistics") {
    contentType = "text/html"
    val salesHitlist = dataStore.getProductHitlist(10, startDate, endDate)
    val worldWideSales = dataStore.getWorldWideSales(startDate, endDate)
    layoutTemplate("/company", "products" -> salesHitlist, "worldWideSales" -> worldWideSales)
  }
}