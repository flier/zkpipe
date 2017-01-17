package zkpipe

import java.io.{Closeable, File, PrintWriter, StringWriter}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.regex.Pattern

import com.codahale.metrics.JmxReporter
import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.hotspot.DefaultExports
import nl.grons.metrics.scala.DefaultInstrumented
import scopt.{OptionParser, Read}
import org.apache.kafka.common.serialization.Serializer

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps, reflectiveCalls}
import scala.concurrent.duration._

object MessageFormats extends Enumeration {
    type MessageFormat = Value

    val pb, json, raw = Value
}

import MessageFormats._

case class Config(@BeanProperty
                  mode: String = null,
                  logFiles: Seq[File] = Seq(),
                  logDir: Option[File] = None,
                  zxidRange: Option[Range] = None,
                  pathPrefix: Option[String] = None,
                  matchPattern: Option[Pattern] = None,
                  sessionId: Option[Long] = None,
                  @BooleanBeanProperty
                  ignoreSession: Boolean = false,
                  @BooleanBeanProperty
                  fromLatest: Boolean = false,
                  @BooleanBeanProperty
                  checkCrc: Boolean = true,
                  kafkaUri: Uri = null,
                  metricServerUri: Option[Uri] = None,
                  pushGatewayAddr: Option[InetSocketAddress] = None,
                  reportUri: Option[Uri] = None,
                  pushInterval: Duration = 15 second,
                  @BooleanBeanProperty
                  jvmMetrics: Boolean = false,
                  @BooleanBeanProperty
                  httpMetrics: Boolean = false,
                  msgFormat: MessageFormat = json)
    extends ConfigMBean with DefaultInstrumented with LazyLogging
{
    lazy val files: Seq[File] = logFiles flatMap { file =>
        if (file.isDirectory)
            file.listFiles
        else if (file.isFile)
            Seq(file)
        else
            Files.newDirectoryStream(file.getParentFile.toPath, file.getName).asScala map { _.toFile }
    } sorted

    lazy val valueSerializer: Serializer[LogRecord] with LazyLogging = msgFormat match {
        case `pb` => new ProtoBufSerializer
        case `json` => new JsonSerializer
        case `raw` => new RawSerializer
    }

    def initializeMetrics(): Seq[Closeable] = {
        // Registry JMV hotspot metrics to Prometheus collector
        if (jvmMetrics) DefaultExports.initialize()

        // Registry DropWizard metrics to Prometheus collector
        new DropwizardExports(metricRegistry).register()

        // Export DropWizard metrics as JMX MBean
        JmxReporter.forRegistry(metricRegistry).build().start()

        // Start HTTP server for Prometheus metrics
        val metricServer = metricServerUri.map({ new MetricServer(_, httpMetrics) })

        // Start pusher task for Prometheus push gateway
        val metricPusher = pushGatewayAddr.map({ new MetricPusher(_, pushInterval) })

        // Report metrics to the Graphite or Ganglia server
        val metricReporter = reportUri.map({ new MetricReporter(_, pushInterval) })

        Seq() ++ metricServer ++ metricPusher ++ metricReporter
    }

    override def getLogFiles: String = logFiles mkString ","

    override def getLogDirectory: String = logDir.map(_.toString).orNull

    override def getZxidRange: String = zxidRange.toString

    override def getPathPrefix: String = pathPrefix.orNull

    override def getMatchPattern: String = matchPattern.map(_.pattern()).orNull

    override def getSessionId: Long = sessionId.getOrElse(-1)

    override def getKafkaUri: String = kafkaUri.toString()

    override def getMetricServerUri: String = metricServerUri.map(_.toString()).orNull

    override def getReportUri: String = reportUri.map(_.toString()).orNull

    override def getPushInterval: Long = pushInterval.toSeconds

    override def getMessageFormat: String = msgFormat.toString
}

object Config {
    def parse(args: Array[String]): Option[Config] = {
        implicit val messageFormatRead: Read[MessageFormat] = Read.reads(MessageFormats withName)
        implicit val uriRead: Read[Uri] = Read.reads(Uri.parse)
        implicit val rangeRead: Read[Option[Range]] = Read.reads(s =>
            s.split(':') match {
                case Array(low) => Some(low.toInt until Int.MaxValue)       // `<low>:`
                case Array("", high) => Some(0 until high.toInt)            // `:<high>`
                case Array(low, high) => Some(low.toInt until high.toInt)   // `<low>:<high>`
                case Array() => Some(Int.MinValue until Int.MaxValue)       // `:`
                case Array("") => None                                      // ``
            }
        )

        val parser = new OptionParser[Config]("zkpipe") {
            head("zkpipe", "0.1")

            help("help").abbr("h").text("show usage screen")

            note("\n[Records]\n")

            opt[Option[Range]]('r', "range")
                .valueName("<zxid:zxid>")
                .action((x, c) => c.copy(zxidRange = x))
                .text("filter transactions those zxid in the range")

            opt[String]('p', "prefix")
                .valueName("<path>")
                .action((x, c) => c.copy(pathPrefix = Some(x)))
                .text("filter transactions those path match prefix")

            opt[String]('m', "match")
                .valueName("<pattern>")
                .action((x, c) => c.copy(matchPattern = Some(Pattern.compile(x))))
                .text("filter transactions those path match pattern")

            opt[Long]('s', "session-id")
                .valueName("<id>")
                .action((x, c) => c.copy(sessionId = Some(x)))
                .text("filter transactions with the session ID")

            opt[Unit]("ignore-session")
                .action((_, c) => c.copy(ignoreSession = true))
                .text("ignore Zookeeper session events")

            opt[Boolean]("check-crc")
                .action((x, c) => c.copy(checkCrc = x))
                .text("check record data CRC correct (default: true)")

            note("\n[Messages]\n")

            opt[MessageFormat]('e', "encode")
                .valueName("<format>")
                .action((x, c) => c.copy(msgFormat = x))
                .text("encode message in [pb, json, raw] format (default: json)")

            opt[Uri]('k', "kafka-uri")
                .valueName("<uri>")
                .action((x, c) => c.copy(kafkaUri = x))
                .validate(c => if (c.scheme.contains("kafka")) success else failure("Option `kafka` scheme should be `kafka://`"))
                .text("sync records to Kafka server (default: console)")

            note("\n[Metrics]\n")

            opt[Uri]('m', "metrics-uri")
                .valueName("<uri>")
                .action((x, c) => c.copy(metricServerUri = Some(x)))
                .validate(c => if (c.scheme.contains("http")) success else failure("Option `metrics` scheme should be `http://`"))
                .text("serve metrics for Prometheus scrape in pull mode")

            opt[String]("push-gateway")
                .valueName("<addr>")
                .action((x, c) => c.copy(pushGatewayAddr = x.split(':') match {
                    case Array(host, port) => Some(new InetSocketAddress(host, port.toInt))
                    case Array(host) => Some(new InetSocketAddress(host, MetricPusher.DEFAULT_PUSH_GATEWAY_PORT))
                    case _ => throw new IllegalArgumentException(s"`$x` is not a valid address")
                }))
                .text("push metrics to the Prometheus push gateway")

            opt[Uri]("report-uri")
                .valueName("<uri>")
                .action((x, c) => c.copy(reportUri = Some(x)))
                .text("report metrics to the Graphite or Ganglia server")

            opt[Duration]("push-interval")
                .action((x, c) => c.copy(pushInterval = x))
                .text("schedule to push metrics (default: 15 seconds)")

            opt[Unit]("jvm-metrics")
                .action((_, c) => c.copy(jvmMetrics = true))
                .text("export JVM hotspot metrics (default: false)")

            opt[Unit]("http-metrics")
                .action((_, c) => c.copy(httpMetrics = true))
                .text("export HTTP metrics (default: false)")

            note("\n[Commands]\n")

            cmd("watch").action( (_, c) => c.copy(mode = "watch") )
                .text("watch Zookeeper binary log changes")
                .children(
                    opt[Unit]("from-latest")
                        .action((_, c) => c.copy(fromLatest = true))
                        .text("sync Zookeeper from latest transaction in binary logs"),

                    arg[File]("<path>")
                        .required()
                        .maxOccurs(1)
                        .action((x, c) => c.copy(logDir = Some(x)))
                        .validate(c => if (c.isDirectory) success else failure("should be a directory"))
                        .text("Zookeeper log directory to monitor changes")
                )

            note("")

            cmd("sync").action( (_, c) => c.copy(mode = "sync") )
                .text("sync Zookeeper binary log files")
                .children(
                    arg[File]("<file>...")
                        .required()
                        .unbounded()
                        .action( (x, c) => c.copy(logFiles = c.logFiles :+ x) )
                        .text("sync Zookeeper log files")
                )
        }

        parser.parse(args, Config())
    }
}

class LogConsole(valueSerializer: Serializer[LogRecord]) extends Broker with LazyLogging {
    case class Result(record: LogRecord) extends SendResult

    override def send(record: LogRecord): Future[SendResult] = {
        println(new String(valueSerializer.serialize("console", record), UTF_8))

        Future successful Result(record)
    }

    override def close(): Unit = {}
}

object LogPipe extends JMXExport with LazyLogging {
    def main(args: Array[String]): Unit = {
        for (config <- Config.parse(args))
        {
            mbean(config)

            var services: Seq[Closeable] = config.initializeMetrics()

            try {
                val changedFiles = config.mode match {
                    case "watch" =>
                        val watcher = new LogWatcher(config.logDir.get,
                                                     checkCrc = config.checkCrc,
                                                     fromLatest = config.fromLatest)

                        services = services :+ watcher

                        watcher.changedFiles
                    case "sync" =>
                        config.files map {
                            new LogFile(_)
                        } sortBy {
                            _.firstZxid
                        }
                }

                var zxidRange = config.zxidRange

                val broker = if (config.kafkaUri != null) {
                    val kafkaBroker = new KafkaBroker(config.kafkaUri, config.valueSerializer)

                    if (!config.fromLatest && config.zxidRange.isEmpty) {
                        val latestZxid = kafkaBroker.latestZxid()

                        zxidRange = latestZxid.map(_.toInt until Int.MaxValue)

                        logger.info(s"auto resume from zxid=$latestZxid")
                    }

                    kafkaBroker
                } else {
                    new LogConsole(config.valueSerializer)
                }

                services = services :+ broker

                run(changedFiles, broker, zxidRange, { record =>
                    zxidRange.forall(_ contains record.zxid.toInt) &&
                    config.pathPrefix.forall({ prefix => record.path.forall(_.startsWith(prefix)) }) &&
                    config.matchPattern.forall({ pattern => record.path.forall(pattern.matcher(_).matches()) }) &&
                    config.sessionId.forall({record.session == _}) &&
                    (!config.ignoreSession || !(Seq(TxnTypes.CreateSession, TxnTypes.CloseSession) contains record.opcode))
                })
            } finally {
                services.foreach(_.close())
            }
        }
    }

    def run(changedFiles: Traversable[LogFile], broker: Broker, zxidRange: Option[Range], matcher: (LogRecord) => Boolean): Unit =
    {
        try {
            changedFiles foreach { logFile =>
                try {
                    for (
                        zxid <- logFile.firstZxid
                        if zxidRange.forall(zxid.toInt < _.end)
                    ) {
                        logger.info(s"sync log file ${logFile.filename} ...")

                        logFile.records filter {
                            matcher
                        } foreach {
                            broker.send
                        }
                    }
                } finally {
                    logFile.close()
                }
            }
        } catch {
            case err: Throwable =>
                logger.error(s"crashed, $err")
                logger.debug({
                    val sw = new StringWriter
                    err.printStackTrace(new PrintWriter(sw))
                    sw.toString
                })
        }
    }
}


