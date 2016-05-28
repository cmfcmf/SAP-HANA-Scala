package de.hpi.callcenterdashboard.controller

import de.hpi.callcenterdashboard.entity.{Region, Money}
import de.hpi.callcenterdashboard.utility.FormattedDate
import org.scalatra.scalate.ScalateSupport

class CompanyServlet extends DataStoreAwareServlet with ScalateSupport with DateAwareServlet {
  // country must be before region!
  val filterAttributes = List("factory", "region", "country", "materialType", "productHierarchyVal", "salesOrganization")

  get("/statistics") {
    contentType = "text/html"
    val cashCowProducts = dataStore.getCashCowProducts(10, startDate, endDate)
    val worldWideSales = dataStore.getWorldWideSales(startDate, endDate)
    var regionSales = List.empty[(String, String, Money, List[(String, String, Money)])]
    for (triple <- worldWideSales) {
      regionSales = regionSales :+ (triple._3, triple._1, triple._2, dataStore.getSalesForRegionsOfCountry(triple._1,
        startDate, endDate))
    }

    filterAttributes.foreach(attribute => {
      templateAttributes(attribute) = session.getAttribute(attribute)
    })

    layoutTemplate("/company/statistics",
      "cashCowProducts" -> cashCowProducts,
      "worldWideSales" -> worldWideSales,
      "regionalSales" -> regionSales,
      "filter" -> Map(
        "factories" -> dataStore.getFactories,
        "countries" -> dataStore.getCountries,
        "regions" -> {
          val country = session.getAttribute("country").asInstanceOf[String]
          if (!Option(country).getOrElse("").isEmpty) {
            val regions = dataStore.getRegionsForCountry(country)
            println(country)
            println(regions)
            regions
          } else List.empty[Region]
        },
        "materialTypes" -> dataStore.getMaterialTypes,
        "productHierarchy" -> dataStore.getProductHierarchy,
        "salesOrganizations" -> dataStore.getSalesOrganizations,
        "indent" -> ((n: String) => "|" + ("-" * n.toInt)),
        "isSelected" -> ((condition: String) => {
          val tmp = condition.split('=')
          if (tmp.length == 2 && session.getAttribute(tmp(0)) == tmp(1)) " selected=\"\"" else ""
        })
        )
    )
  }

  post("/statistics/filter") {
    filterAttributes.foreach(attribute => {
      params.get(attribute).foreach(attributeValue => {
        if (attribute == "country" && attributeValue != session.getAttribute("country")) {
          session.removeAttribute("region")
        }
        if (attributeValue.nonEmpty)
          session.setAttribute(attribute, attributeValue)
        else
          session.removeAttribute(attribute)
      })
    })
    redirect(fullUrl("/statistics"))
  }
}