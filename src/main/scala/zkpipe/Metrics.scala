package zkpipe

import java.io.Closeable
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.{Timer, TimerTask}

import com.codahale.metrics.{MetricFilter, MetricRegistry, ScheduledReporter}
import com.codahale.metrics.ganglia.GangliaReporter
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.health.HealthCheckRegistry
import com.codahale.metrics.jetty9.InstrumentedHandler
import com.codahale.metrics.servlets.{AdminServlet, HealthCheckServlet}
import com.codahale.metrics.servlets.MetricsServlet.ContextListener
import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import info.ganglia.gmetric4j.gmetric.GMetric
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.{MetricsServlet, PushGateway}
import nl.grons.metrics.scala.DefaultInstrumented
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import scala.async.Async.async
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class MetricServer(uri: Uri, httpMetrics: Boolean) extends DefaultInstrumented with LazyLogging with Closeable {
    val DEFAULT_HOST = "localhost"
    val DEFAULT_PORT = 9091

    val addr = new InetSocketAddress(uri.host.getOrElse(DEFAULT_HOST), uri.port.getOrElse(DEFAULT_PORT))
    val server: Server = new Server(addr)
    val context: ServletContextHandler = new ServletContextHandler

    context.setContextPath("/")
    context.addServlet(new ServletHolder(new MetricsServlet()), uri.path)

    context.addEventListener(new ContextListener {
        override def getMetricRegistry: MetricRegistry = metricRegistry
    })
    context.addEventListener(new HealthCheckServlet.ContextListener {
        override def getHealthCheckRegistry: HealthCheckRegistry = registry
    })
    context.addServlet(new ServletHolder(new AdminServlet()), "/dropwizard/*")

    if (httpMetrics) {
        val instrumented = new InstrumentedHandler(metricRegistry)

        instrumented.setName("jetty")
        instrumented.setHandler(context)

        server.setHandler(instrumented)
    } else {
        server.setHandler(context)
    }

    logger.info(s"serve metrics @ $uri")

    server.start()

    healthCheck("alive") { server.isRunning }

    override def close(): Unit = {
        logger.info("stop serve metrics")

        server.stop()
    }
}

class MetricPusher(addr: InetSocketAddress,
                   interval: Duration,
                   jobName: String = "zkpipe",
                   collectorRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry)
    extends DefaultInstrumented with LazyLogging with Closeable
{
    val host = s"${addr.getHostString}:${addr.getPort}"
    val gateway = new PushGateway(host)

    var timer = new Timer

    private val pushTimes = metrics.counter("push_times")
    private val pushElapsed = metrics.timer("push_elapsed")

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

        pushElapsed.time {
            if (all)
                gateway.push(collectorRegistry, jobName)
            else
                gateway.pushAdd(collectorRegistry, jobName)
        }

        pushTimes += 1
    }

    override def close(): Unit = {
        logger.info("stop to push metrics")

        timer.cancel()

        gateway.delete(jobName)
    }
}

class MetricReporter(uri: Uri, interval: Duration, prefix: String = "zkpipe")
    extends DefaultInstrumented with LazyLogging with Closeable
{
    val DEFAULT_HOST = "localhost"
    val DEFAULT_GRAPHITE_PORT = 2003
    val DEFAULT_PICKLE_PORT = 2003
    val DEFAULT_GANGLIA_PORT = 8649

    val addr = new InetSocketAddress(uri.host.get, uri.port.get)
    val reporter: ScheduledReporter = uri.scheme.get match {
        case "graphite" =>
            val host = uri.host.getOrElse(DEFAULT_HOST)
            val port = uri.port.getOrElse(DEFAULT_GRAPHITE_PORT)
            val graphite = new Graphite(new InetSocketAddress(host, port))

            logger.info(s"report metrics to graphite://$host:$port")

            GraphiteReporter.forRegistry(metricRegistry)
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite)

        case "pickle" =>
            val host = uri.host.getOrElse(DEFAULT_HOST)
            val port = uri.port.getOrElse(DEFAULT_PICKLE_PORT)
            val graphite = new Graphite(new InetSocketAddress(host, port))

            logger.info(s"report metrics to pickle://$host:$port")

            GraphiteReporter.forRegistry(metricRegistry)
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite)

        case "ganglia" =>
            val host = uri.host.getOrElse(DEFAULT_HOST)
            val port = uri.port.getOrElse(DEFAULT_GANGLIA_PORT)
            val ganglia = new GMetric(host, port, UDPAddressingMode.MULTICAST, 1)

            logger.info(s"report metrics to pickle://$host:$port")

            GangliaReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(ganglia)

        case _ =>
            throw new Exception(s"unknown reporter scheme: ${uri.scheme}")
    }

    reporter.start(interval.toSeconds, TimeUnit.SECONDS)

    override def close(): Unit = {
        logger.info("stop to report metrics")

        reporter.stop()
    }
}