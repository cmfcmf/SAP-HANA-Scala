package de.hpi.callcenterdashboard

import org.scalatra._

class CallcenterServlet extends CallcenterDashboardStack {

  get("/") {
    <html>
      <body>
        <h1>Hello, world!</h1>
        Say <a href="hello-scalate">hello to Scalate</a>.
      </body>
    </html>
  }

}
