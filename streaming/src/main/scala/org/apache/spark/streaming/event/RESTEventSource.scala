package org.apache.spark.streaming.event

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

import spark.{Request, Response, Route, Service}

import org.apache.spark.streaming.{StreamingContext, Time}

case class RESTRequest(
    attributes: Set[String],
    body: String,
    contentLength: Int,
    contentType: String,
    contextPath: String,
    cookies: Map[String, String],
    headers: Set[String],
    host: String,
    ip: String,
    params: Map[String, String],
    pathInfo: String,
    port: Int,
    protocol: String,
    queryMap: Map[String, Array[String]],
    queryString: String,
    requestMethod: String,
    scheme: String,
    servletPath: String,
    splat: Array[String],
    uri: String,
    url: String,
    userAgent: String
)

case class RESTEvent(
    source: EventSource,
    request: RESTRequest,
    override val index: Long,
    override val time: Time
  ) extends Event(source, index, time)

class RESTEventSource(
    @transient ssc: StreamingContext,
    address: String,
    port: Int,
    name: String,
    maxThreads: Int = -1,
    minThreads: Int = -1,
    threadIdleTimeoutMillis: Int = -1
  ) extends EventSource(ssc, name) {

  private var stopped = true
  private val indexCounter = new AtomicLong(0L)
  @transient private var restService: Option[Service] = None
  private val operations = ListBuffer.empty[(Service, Route => Route) => Unit]

  override def start(): Unit = synchronized {
    require(stopped || restService.isEmpty,
      s"${this.toString} is already started.")

    restService = Some(Service.ignite())

    restService.foreach { rest =>
      rest.ipAddress(address).port(port)
        .threadPool(maxThreads, minThreads, threadIdleTimeoutMillis)
      operations.foreach(_(rest, route))
      rest.init()
    }

    stopped = false

    logDebug(s"${this.toString} " +
      s"started on address $address and port $port.")
  }

  override def restart() = start()

  override def stop(): Unit = synchronized {
    require(!stopped, s"${this.toString} already stopped.")
    restService.foreach(_.stop())
    restService = None
    stopped = true
    logDebug(s"${this.toString} stopped")
  }

  override def toProduct: Product = (address, port, name)

  override def toDetailedString: String = {
    s"REST($name,$address:$port)"
  }

  def withRESTService(op: (Service, Route => Route) => Unit): RESTEventSource = {
    require(stopped, s"${this.toString} should be stopped first.")
    operations += op
    this
  }

  private def route(route: Route): Route = new Route {
    import RESTEventSource._
    override def handle(request: Request, response: Response): AnyRef = {
      val time = Time(context.clock.getTimeMillis)
      val res = route.handle(request, response)
      val event = RESTEvent(RESTEventSource.this, request,
        indexCounter.getAndIncrement(), time)
      post(event)
      logDebug(s"${this.toString} emitted event $event")
      res
    }
  }

  def get(path: String): RESTEventSource = get(path, RESTEventSource.defaultRoute)

  def get(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.get(path, routeFunc(route))
  }

  def post(path: String): RESTEventSource = post(path, RESTEventSource.defaultRoute)

  def post(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.post(path, routeFunc(route))
  }

  def put(path: String): RESTEventSource = put(path, RESTEventSource.defaultRoute)

  def put(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.put(path, routeFunc(route))
  }

  def delete(path: String): RESTEventSource = delete(path, RESTEventSource.defaultRoute)

  def delete(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.delete(path, routeFunc(route))
  }

  def connect(path: String): RESTEventSource = connect(path, RESTEventSource.defaultRoute)

  def connect(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.connect(path, routeFunc(route))
  }

  def head(path: String): RESTEventSource = head(path, RESTEventSource.defaultRoute)

  def head(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.head(path, routeFunc(route))
  }

  def options(path: String): RESTEventSource = options(path, RESTEventSource.defaultRoute)

  def options(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.options(path, routeFunc(route))
  }

  def patch(path: String): RESTEventSource = patch(path, RESTEventSource.defaultRoute)

  def patch(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.patch(path, routeFunc(route))
  }

  def trace(path: String): RESTEventSource = trace(path, RESTEventSource.defaultRoute)

  def trace(path: String, route: Route): RESTEventSource = withRESTService {
    (rest, routeFunc) => rest.trace(path, routeFunc(route))
  }

}

object RESTEventSource {

  val defaultRoute = new Route {
    override def handle(request: Request, response: Response): AnyRef = ""
  }

  implicit def reqResToRotue(reqRes: (Request, Response) => AnyRef): Route = {
    new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        reqRes(request, response)
      }
    }
  }

  implicit def requestToRESTRequest(request: Request): RESTRequest = {
    RESTRequest(
      JavaConversions.asScalaSet(request.attributes()).toSet,
      request.body,
      request.contentLength,
      request.contentType,
      request.contextPath,
      JavaConversions.mapAsScalaMap(request.cookies).toMap,
      JavaConversions.asScalaSet(request.headers).toSet,
      request.host,
      request.ip,
      JavaConversions.mapAsScalaMap(request.params).toMap,
      request.pathInfo,
      request.port,
      request.protocol,
      JavaConversions.mapAsScalaMap(request.raw.getParameterMap).toMap,
      request.queryString,
      request.requestMethod,
      request.scheme,
      request.servletPath,
      request.splat,
      request.uri,
      request.url,
      request.userAgent)
  }

}
