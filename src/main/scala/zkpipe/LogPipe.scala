package zkpipe

import java.io.{Closeable, File, PrintWriter, StringWriter}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.hotspot.DefaultExports
import scopt.{OptionParser, Read}
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps}
import scala.concurrent.duration._

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
                  metricServerUri: Option[Uri] = None,
                  pushGatewayAddr: Option[InetSocketAddress] = None,
                  pushInterval: Duration = 15 second,
                  jvmMetrics: Boolean = false,
                  msgFormat: MessageFormat = json) extends LazyLogging
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
        implicit val addrRead: Read[InetSocketAddress] = Read.reads( addr =>
            addr.split(':') match {
                case Array(host, port) => new InetSocketAddress(host, port.toInt)
                case Array(host) => new InetSocketAddress(host, 9091)
                case _ => throw new IllegalArgumentException(s"`$addr` is not a valid address")
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
                .action((_, c) => c.copy(fromLatest = true))
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

            opt[Uri]('m', "metrics-uri")
                .valueName("<uri>")
                .action((x, c) => c.copy(metricServerUri = Some(x)))
                .validate(c => if (c.scheme.contains("http")) success else failure("Option `metrics` scheme should be `http://`"))
                .text("serve metrics for prometheus")

            opt[InetSocketAddress]("push-gateway")
                .valueName("<addr>")
                .action((x, c) => c.copy(pushGatewayAddr = Some(x)))
                .text("push metrics to the prometheus push gateway")

            opt[Duration]("push-interval")
                .action((x, c) => c.copy(pushInterval = x))
                .text("schedule to push metrics (default: 15 seconds)")

            opt[Boolean]("jvm-metrics")
                .action((x, c) => c.copy(jvmMetrics = x))
                .text("provides JVM hotspot metrics (default: false)")

            opt[MessageFormat]('f', "format")
                .valueName("<format>")
                .action((x, c) => c.copy(msgFormat = x))
                .text("serialize message in [pb, json, raw] format (default: json)")

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
            if (config.jvmMetrics) DefaultExports.initialize()

            val metricServer = config.metricServerUri.map(MetricServer)
            val metricPusher = config.pushGatewayAddr.map({ MetricPusher(_, config.pushInterval) })

            var services: Seq[Closeable] = Seq() ++ metricServer ++ metricPusher

            try {
                val changedFiles = config.logDir match {
                    case Some(dir) =>
                        val watcher = new LogWatcher(dir, checkCrc = config.checkCrc, fromLatest = config.fromLatest)

                        services = services :+ watcher

                        watcher.changedFiles
                    case _ =>
                        config.files map {
                            new LogFile(_)
                        }
                }

                val broker = if (config.kafkaUri != null) {
                    new LogBroker(config.kafkaUri, config.valueSerializer)
                } else {
                    new LogConsole(config.valueSerializer)
                }

                services = services :+ broker

                run(changedFiles,
                    broker,
                    zxidRange = config.zxidRange,
                    pathPrefix = config.pathPrefix)
            } finally {
                logger.info("closing services")

                services.reverse.foreach(_.close())
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


