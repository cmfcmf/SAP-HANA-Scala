package de.hpi.callcenterdashboard

import org.scalatra.scalate.ScalateSupport

class CallcenterServlet extends DataStoreAwareServlet with ScalateSupport {
  get("/") {
    layoutTemplate("/index")
  }
  post("/find-customer") {
    val customers = dataStore.getCustomersBy(params("customerId"), params("customerName"), params("customerZip"))
    layoutTemplate("/find-customer", "customers" -> customers)
  }

  get("/customer/:id") {
    val customerId = params("id")
    val customer = dataStore.getSingleCustomerById(customerId)
    customer.map(customer => {
      // We found a customer for the given id.
      val orders = dataStore.getOrdersOf(customer)
      val sales = dataStore.getSalesAndProfitOf(customer)
      layoutTemplate("/customer", "customer" -> customer, "orders" -> orders, "sales" -> sales)
    }).getOrElse(resourceNotFound)
  }

  get("/about") {
    layoutTemplate("/about")
  }
}
