package de.hpi.callcenterdashboard

import org.scalatra._
import org.scalatra.scalate.ScalateSupport

class CallcenterServlet extends CallcenterDashboardStack with ScalateSupport {

  val connection = new DatabaseConnection()

  get("/") {
    contentType="text/html"

    layoutTemplate("/index")
  }
  post("/find-customer") {

  }

}
