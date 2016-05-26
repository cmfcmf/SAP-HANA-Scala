import javax.servlet.ServletContext

import de.hpi.callcenterdashboard.controller.{CompanyServlet, CustomerServlet}
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    context.mount(new CompanyServlet, "/company/*")
    context.mount(new CustomerServlet, "/*")
  }
}
