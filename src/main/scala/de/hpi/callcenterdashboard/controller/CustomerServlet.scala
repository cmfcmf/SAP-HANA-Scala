package de.hpi.callcenterdashboard.controller

import org.scalatra.scalate.ScalateSupport

class CustomerServlet extends DataStoreAwareServlet with ScalateSupport with DateAwareServlet {
  get("/") {
    contentType = "text/html"
    layoutTemplate("/customer/search")
  }

  get("/find-customer") {
    contentType = "text/html"
    val customers = dataStore.getCustomersBy(params("customerId"), params("customerName"), params("customerZip"))
    if (customers.length == 1) {
      // If there only is a single result, redirect user to the customer details page immediately.
      redirect("/customer/" + customers.head.customerId)
    } else {
      layoutTemplate("/customer/search-results", "customers" -> customers)
    }
  }

  get("/customer/:id") {
    contentType = "text/html"

    val customerId = params("id")
    val customer = dataStore.getSingleCustomerById(customerId)
    customer.map(customer => {
      // We found a customer for the given id.
      val orders = dataStore.getOrdersOf(customer)
      val sales = dataStore.getSalesAndProfitOf(customer)
      val outstanding_orders = dataStore.getOutstandingOrdersOfCustomerUpTo(customer, endDate)
      val salesContributions = dataStore.getProductSalesPercent(
        customerId,
        startDate,
        endDate
      )
      salesContributions.foreach {println}
      layoutTemplate(
        "/customer/customer",
        "customer" -> customer,
        "orders" -> orders,
        "sales" -> sales,
        "outstanding_orders" -> outstanding_orders,
        "outstanding_orders_date" -> endDate,
        "sales_contributions" -> salesContributions
      )
    }).getOrElse(resourceNotFound)
  }

  get("/about") {
    contentType = "text/html"
    layoutTemplate("/about")
  }
}
