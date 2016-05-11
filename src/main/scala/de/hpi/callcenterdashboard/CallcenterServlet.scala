package de.hpi.callcenterdashboard

import javax.servlet.ServletConfig
import org.scalatra.scalate.ScalateSupport

class CallcenterServlet extends CallcenterDashboardStack with ScalateSupport {
  get("/") {
    contentType="text/html"

    layoutTemplate("/index")
  }
  post("/find-customer") {
    contentType="text/html"
    val customerList = databaseConnection.getCustomerBy(params("customerId"), params("customerName"), params("customerZip"))
    layoutTemplate("/customer-search", "customers" -> customerList)
  }

  get("/customer/:id") {
    contentType="text/html"
    val customerId = params("id")
    val customer = databaseConnection.getSingleCustomerBy(customerId)
    val orders = databaseConnection.getOrdersOf(customerId)
    val sales = databaseConnection.getSalesAndProfitOf(customerId, List("2013", "2014"))
    layoutTemplate("/customer-details", "customer" -> customer, "orders" -> orders, "sales" -> sales)
  }

  val databaseConnection = new DatabaseConnection()

  override def init(config: ServletConfig): Unit = {
    super.init(config)

    databaseConnection.open()
  }

  override def destroy(): Unit = {
    super.destroy()

    databaseConnection.close()
  }
}
