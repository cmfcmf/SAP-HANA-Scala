package de.hpi.callcenterdashboard

import javax.servlet.ServletConfig
import org.scalatra.scalate.ScalateSupport

class CallcenterServlet extends CallcenterDashboardStack with ScalateSupport {
  get("/") {
    contentType="text/html"

    layoutTemplate("/index")
  }
  post("/find-customer") {
    databaseConnection.open()
    databaseConnection.printCustomers()
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
