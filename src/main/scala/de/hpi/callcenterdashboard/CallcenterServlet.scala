package de.hpi.callcenterdashboard

import org.scalatra.scalate.ScalateSupport

class CallcenterServlet extends DataStoreAwareServlet with ScalateSupport {
  get("/") {
    contentType="text/html"
    layoutTemplate("/index")
  }
  post("/find-customer") {
    contentType = "text/html"
    val customers = dataStore.getCustomersBy(params("customerId"), params("customerName"), params("customerZip"))
    layoutTemplate("/customer-search", "customers" -> customers)
  }

  get("/customer/:id") {
    contentType = "text/html"
    val customerId = params("id")
    val customer = dataStore.getSingleCustomerById(customerId)
    customer.map(customer => {
      // We found a customer for the given id.
      val orders = dataStore.getOrdersOf(customer)
      val sales = dataStore.getSalesAndProfitOf(customer)
      layoutTemplate("/customer-details", "customer" -> customer, "orders" -> orders, "sales" -> sales)
    }).getOrElse(resourceNotFound)
  }
}
