package de.hpi.callcenterdashboard.controller

import de.hpi.callcenterdashboard.entity.Money
import org.scalatra.scalate.ScalateSupport

class CompanyServlet extends DataStoreAwareServlet with ScalateSupport with DateAwareServlet {
  get("/statistics") {
    contentType = "text/html"
    val salesHitlist = dataStore.getProductHitlist(10, startDate, endDate)
    val worldWideSales = dataStore.getWorldWideSales(startDate, endDate)
    var regionSales = List.empty[(String, String, Money, List[(String, String, Money)])]
    for (triple <- worldWideSales) {
      regionSales = regionSales :+ (triple._3, triple._1, triple._2, dataStore.getSalesForRegionsOfCountry(triple._1,
        startDate, endDate))
    }
    layoutTemplate("/company/statistics",
      "products" -> salesHitlist,
      "worldWideSales" -> worldWideSales,
      "regionalSales" -> regionSales)
  }
}