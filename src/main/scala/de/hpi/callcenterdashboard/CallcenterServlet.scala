package de.hpi.callcenterdashboard

import de.hpi.utility.FormattedDate
import java.text.SimpleDateFormat
import java.util.Date

import org.scalatra.{ScalatraParams, SessionSupport}
import org.scalatra.scalate.ScalateSupport

class CallcenterServlet extends DataStoreAwareServlet with ScalateSupport with SessionSupport {
  before() {
    if (params.getOrElse('startDate, "").nonEmpty) session.setAttribute("startDate", params('startDate))
    if (params.getOrElse('endDate, "").nonEmpty) session.setAttribute("endDate", params('endDate))

    templateAttributes("startDate") = session.getAttribute("startDate")
    templateAttributes("endDate") = session.getAttribute("endDate")
    templateAttributes("isGetRequest") = request.getMethod == "GET"
  }

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

  get("/customer/:id/:outstanding_orders_date/?") {
    contentType = "text/html"
    /*
    val date : Date = new Date()
    val sdf : SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val formattedDate : String = sdf.format(date)
    */
    val formattedDate = FormattedDate.today_yyyyMMdd()
    val outstanding_orders_date : String = params.getOrElse("outstanding_orders_date", formattedDate)

    val tempDate = new SimpleDateFormat("yyyyMMdd").parse(outstanding_orders_date)
    val outstanding_orders_date_well_formed = new SimpleDateFormat("yyyy/MM/dd").format(tempDate)

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
                      "outstanding_orders_date_well_formed" -> outstanding_orders_date_well_formed)
    }).getOrElse(resourceNotFound)
  }

  get("/about") {
    contentType = "text/html"
    layoutTemplate("/about")
  }
}
