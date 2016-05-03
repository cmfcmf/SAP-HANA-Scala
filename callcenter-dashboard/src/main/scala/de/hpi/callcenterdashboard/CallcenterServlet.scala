package de.hpi.callcenterdashboard

import org.scalatra._
import org.scalatra.scalate.ScalateSupport

class CallcenterServlet extends CallcenterDashboardStack with ScalateSupport {

  get("/") {
    contentType="text/html"

    layoutTemplate("/index")
  }

}
