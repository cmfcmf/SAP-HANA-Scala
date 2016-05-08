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

  get("/example-1") {
    contentType = "text/html"

    class AnObject {
      def aMethod = "Hi there!"
    }

    val list = List("foo", "bar", "baz")

    layoutTemplate("/example1", "html" -> "<b>I am fat.</b>", "object" -> new AnObject(), "list" -> list)
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
