package zkpipe

import java.io.{File, PrintWriter, StringWriter}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.exporter.MetricsServlet
import io.prometheus.client.hotspot.DefaultExports
import scopt.{OptionParser, Read}
import org.apache.kafka.common.serialization.Serializer
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.postfixOps

object MessageFormats extends Enumeration {
    type MessageFormat = Value

    val pb, json, raw = Value
}

import MessageFormats._

case class Config(logFiles: Seq[File] = Seq(),
                  logDir: Option[File] = None,
                  zxidRange: Range = 0 until Int.MaxValue,
                  fromLatest: Boolean = false,
                  pathPrefix: String = "/",
                  checkCrc: Boolean = true,
                  kafkaUri: Uri = null,
                  metricUri: Uri = null,
                  jvmMetrics: Boolean = false,
                  msgFormat: MessageFormat = pb) extends LazyLogging
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
}

object Config {
    def parse(args: Array[String]): Option[Config] = {
        implicit val messageFormatRead: Read[MessageFormat] = Read.reads(MessageFormats withName)
        implicit val uriRead: Read[Uri] = Read.reads(Uri.parse)
        implicit val rangeRead: Read[Range] = Read.reads(s =>
            s.split(':') match {
                case Array(low) => low.toInt until Int.MaxValue
                case Array("", upper) => 0 until upper.toInt
                case Array(low, upper) => low.toInt until upper.toInt
                case Array() => Int.MinValue until Int.MaxValue
            }
        )

        val parser = new OptionParser[Config]("zkpipe") {
            head("zkpipe", "0.1")

            opt[File]('d', "log-dir")
                .valueName("<path>")
                .action((x, c) => c.copy(logDir = Some(x)))
                .validate(c => if (c.isDirectory) success else failure("Option `log-dir` should be a directory"))
                .text("Zookeeper log directory to monitor changes")

            opt[Range]('r', "range")
                .valueName("<zxid:zxid>")
                .action((x, c) => c.copy(zxidRange = x))
                .text("sync Zookeeper transactions with id in the range (default `:`)")

            opt[Unit]("from-latest")
                .action((x, c) => c.copy(fromLatest = true))
                .text("sync Zookeeper from latest transaction")

            opt[String]('p', "prefix")
                .valueName("<path>")
                .action((x, c) => c.copy(pathPrefix = x))
                .text("sync Zookeeper transactions with path prefix (default: `/`)")

            opt[Boolean]("check-crc")
                .action((x, c) => c.copy(checkCrc = x))
                .text("check record data CRC correct (default: true)")

            opt[Uri]('k', "kafka")
                .valueName("<uri>")
                .action((x, c) => c.copy(kafkaUri = x))
                .validate(c => if (c.scheme.contains("kafka")) success else failure("Option `kafka` scheme should be `kafka://`"))
                .text("sync records to Kafka")

            opt[Uri]('m', "metrics")
                .valueName("<uri>")
                .action((x, c) => c.copy(metricUri = x))
                .validate(c => if (c.scheme.contains("http")) success else failure("Option `metrics` scheme should be `http://`"))
                .text("serve metrics for prometheus")

            opt[Boolean]("jvm-metrics")
                .action((x, c) => c.copy(jvmMetrics = x))
                .text("provides JVM hotspot metrics (default: false)")

            opt[MessageFormat]('f', "format")
                .valueName("<format>")
                .action((x, c) => c.copy(msgFormat = x))
                .text("serialize message in [pb, json, raw] format (default: pb)")

            help("help").abbr("h").text("show usage screen")

            arg[File]("<file>...")
                .unbounded()
                .optional()
                .action( (x, c) => c.copy(logFiles = c.logFiles :+ x) )
                .text("sync Zookeeper log files")
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

object LogPipe extends LazyLogging {
    def main(args: Array[String]): Unit = {
        for (config <- Config.parse(args))
        {
            exportMetrics(config.metricUri, config.jvmMetrics)

            val changedFiles = config.logDir match {
                case Some(dir) =>
                    new LogWatcher(dir, checkCrc = config.checkCrc, fromLatest = config.fromLatest).changedFiles
                case _ =>
                    config.files map { new LogFile(_) }
            }

            val broker = if (config.kafkaUri != null) {
                new LogBroker(config.kafkaUri, config.valueSerializer)
            } else {
                new LogConsole(config.valueSerializer)
            }

            run(changedFiles,
                broker,
                zxidRange = config.zxidRange,
                pathPrefix = config.pathPrefix)
        }
    }

    def exportMetrics(uri: Uri, jvmMetrics: Boolean): Unit = {
        if (uri != null) {
            if (jvmMetrics) DefaultExports.initialize()

            for (
                host <- uri.host;
                port <- uri.port
            ) {
                val server: Server = new Server(new InetSocketAddress(host, port))
                val context: ServletContextHandler = new ServletContextHandler

                context.setContextPath("/")
                context.addServlet(new ServletHolder(new MetricsServlet()), uri.path)

                server.setHandler(context)
                server.start()

                logger.info(s"serve metrics @ ${server.getURI}")
            }
        }
    }

    def run(changedFiles: Traversable[LogFile], broker: Broker, zxidRange: Range, pathPrefix: String): Unit = {
        try {
            changedFiles foreach { logFile =>
                if (logFile.firstZxid.exists(_ < zxidRange.end)) {
                    logger.info(s"sync log file ${logFile.filename} ...")

                    logFile.records filter { r =>
                        (zxidRange contains r.zxid.toInt) && r.path.forall(_.startsWith(pathPrefix))
                    } foreach { r =>
                        broker.send(r)
                    }
                } else {
                    logger.info(s"skip log file ${logFile.filename} not in $zxidRange")

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


