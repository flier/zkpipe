package zkpipe

import java.io.Closeable
import java.net.InetSocketAddress
import java.util.{Timer, TimerTask}

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.{MetricsServlet, PushGateway}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import scala.async.Async.async
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

case class MetricServer(uri: Uri) extends LazyLogging with Closeable {
    val DEFAULT_HOST = "localhost"
    val DEFAULT_PORT = 9091

    val addr = new InetSocketAddress(uri.host.getOrElse(DEFAULT_HOST), uri.port.getOrElse(DEFAULT_PORT))
    val server: Server = new Server(addr)
    val context: ServletContextHandler = new ServletContextHandler

    context.setContextPath("/")
    context.addServlet(new ServletHolder(new MetricsServlet()), uri.path)

    server.setHandler(context)

    logger.info(s"serve metrics @ $uri")

    server.start()

    override def close(): Unit = {
        logger.info("stop serve metrics")

        server.stop()
    }
}

case class MetricPusher(addr: InetSocketAddress,
                        interval: Duration,
                        jobName: String = "zkpipe",
                        registry: CollectorRegistry = CollectorRegistry.defaultRegistry)
    extends LazyLogging with Closeable
{
    val host = s"${addr.getHostString}:${addr.getPort}"

    val gateway = new PushGateway(host)

    var timer = new Timer

    async {
        push(true)
    } andThen {
        case Success(_) =>
            logger.debug(s"schedule to push metrics per $interval")

            Try(timer.schedule(new TimerTask {
                def run(): Unit = push(false)
            }, interval.toMillis, interval.toMillis))
        case Failure(err) =>
            logger.warn(s"fail to push metrics to push gateway @ $addr, $err")
    }

    def push(all: Boolean): Unit = {
        logger.debug("push metrics")

        if (all)
            gateway.push(registry, jobName)
        else
            gateway.pushAdd(registry, jobName)
    }

    override def close(): Unit = {
        logger.info("stop to push metrics")

        timer.cancel()

        gateway.delete(jobName)
    }
}