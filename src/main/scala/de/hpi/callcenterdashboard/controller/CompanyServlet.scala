package de.hpi.callcenterdashboard.controller

import de.hpi.callcenterdashboard.Filter
import de.hpi.callcenterdashboard.entity.Region
import org.scalatra.scalate.ScalateSupport

class CompanyServlet extends DataStoreAwareServlet with ScalateSupport with DateAwareServlet {
  // country must be before region!
  val filterAttributes = List("factory", "region", "country", "materialType", "productHierarchyVal", "salesOrganization")

  get("/statistics") {
    contentType = "text/html"

    val filter = Filter(
      startDate,
      endDate,
      Option(session.getAttribute("factory").asInstanceOf[String]).getOrElse(""),
      Option(session.getAttribute("salesOrganization").asInstanceOf[String]).getOrElse(""),
      Option(session.getAttribute("materialType").asInstanceOf[String]).getOrElse(""),
      Option(session.getAttribute("productHierarchyVal").asInstanceOf[String]).getOrElse(""),
      Option(session.getAttribute("country").asInstanceOf[String]).getOrElse(""),
      Option(session.getAttribute("region").asInstanceOf[String]).getOrElse("")
    )
    val cashCowProducts = dataStore.getCashCowProducts(10, filter)
    val worldWideSales = dataStore.getWorldWideSales(filter)

    filterAttributes.foreach(attribute => {
      templateAttributes(attribute) = session.getAttribute(attribute)
    })

    layoutTemplate("/company/statistics",
      "cashCowProducts" -> cashCowProducts,
      "worldWideSales" -> worldWideSales,
      "filter" -> filterForTemplate
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

  private def filterForTemplate: Map[String, (Int with String) => Object] = {
    Map(
      "factories" -> dataStore.getFactories,
      "countries" -> dataStore.getCountries,
      "regions" -> {
        val country = session.getAttribute("country").asInstanceOf[String]
        if (!Option(country).getOrElse("").isEmpty) dataStore.getRegionsForCountry(country)
        else List.empty[Region]
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
  }
}