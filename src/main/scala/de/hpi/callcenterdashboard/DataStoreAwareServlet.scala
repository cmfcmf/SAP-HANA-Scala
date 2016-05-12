package de.hpi.callcenterdashboard

import javax.servlet.ServletConfig

import org.scalatra.ScalatraServlet
import org.scalatra.scalate.ScalateSupport

trait DataStoreAwareServlet extends ScalatraServlet with ScalateSupport {
  val dataStore = new DataStore()

  override def init(config: ServletConfig): Unit = {
    dataStore.open()

    super.init(config)
  }

  override def destroy(): Unit = {
    dataStore.close()

    super.destroy()
  }

  before() {
    if (!dataStore.isOpened) {
      templateAttributes("error") = "No connection to database!"
    }
    templateAttributes("isCurrentPage") = (path: String) => if (path == requestPath) "active" else ""
  }
}
