package de.hpi.callcenterdashboard

import de.hpi.utility._

import org.scalatra.{ScalatraParams, SessionSupport}
import org.scalatra.scalate.ScalateSupport

class CallcenterServlet extends DataStoreAwareServlet with ScalateSupport with SessionSupport with DateAwareServlet {
  get("/") {
    contentType = "text/html"
    layoutTemplate("/index")
  }
  post("/find-customer") {
    contentType = "text/html"
    val customers = dataStore.getCustomersBy(params("customerId"), params("customerName"), params("customerZip"))
    if (customers.length == 1) {
      // If there only is a single result, redirect user to the customer details page immediately.
      redirect("/customer/" + customers.head.customerId)
    } else {
      layoutTemplate("/find-customer", "customers" -> customers)
    }
  }

  get("/customer/:id/") {
    contentType = "text/html"

    val outstanding_orders_date : FormattedDate = new FormattedDate(
      params.getOrElse(
        "outstanding_orders_date",
        DateFormatter.today().unformatted
      )
    )

    val customerId = params("id")
    val customer = dataStore.getSingleCustomerById(customerId)
    customer.map(customer => {
      // We found a customer for the given id.
      val orders = dataStore.getOrdersOf(customer)
      val sales = dataStore.getSalesAndProfitOf(customer)
      val outstanding_orders = dataStore.getOutstandingOrdersOfCustomerUpTo(customer, outstanding_orders_date)
      layoutTemplate( "/customer",
                      "customer" -> customer,
                      "orders" -> orders,
                      "sales" -> sales,
                      "outstanding_orders" -> outstanding_orders,
                      "outstanding_orders_date" -> outstanding_orders_date,
                      "outstanding_orders_date_well_formed" -> outstanding_orders_date.toString)
    }).getOrElse(resourceNotFound)
  }

  get("/about") {
    contentType = "text/html"
    layoutTemplate("/about")
  }
}
