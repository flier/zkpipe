package zkpipe

import java.io.{File, PrintWriter, StringWriter}

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import scopt.{OptionParser, Read}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.kafka.common.serialization.Serializer

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
                  pathPrefix: String = "/",
                  checkCrc: Boolean = true,
                  kafkaUri: Uri = null,
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
            }
        )

        val parser = new OptionParser[Config]("zkpipe") {
            head("zkpipe", "0.1")

            opt[File]('d', "log-dir")
                .valueName("<path>")
                .action((x, c) => c.copy(logDir = Some(x)))
                .validate(c => if (c.isDirectory) success else failure("Option `log-dir` should be a directory"))
                .text("Zookeeper log directory")

            opt[Range]('r', "range")
                .valueName("<zxid>")
                .action((x, c) => c.copy(zxidRange = x))
                .text("sync ZooKeeper transactions with id in the range")

            opt[String]('p', "prefix")
                .valueName("<path>")
                .action((x, c) => c.copy(pathPrefix = x))
                .text("sync Zookeeper transactions with path prefix")

            opt[Boolean]("check-crc")
                .action((x, c) => c.copy(checkCrc = x))
                .text("check record data CRC correct")

            opt[Uri]('k', "kafka")
                .valueName("<uri>")
                .action((x, c) => c.copy(kafkaUri = x))
                .validate(c => if (c.scheme.contains("kafka")) success else failure("Option `kafka` scheme should be `kafka://`"))
                .text("sync records to Kafka")

            opt[MessageFormat]('f', "msg-format")
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
    case class ConsoleMetadata(log: LogRecord) extends LogMetadata

    override def send(log: LogRecord): Future[LogMetadata] = {
        println(new String(valueSerializer.serialize("console", log), UTF_8))

        Future successful ConsoleMetadata(log)
    }
}

object LogPipe extends LazyLogging {
    def main(args: Array[String]): Unit = {
        for (config <- Config.parse(args))
        {
            val changedFiles = config.logDir match {
                case Some(dir) => new LogWatcher(dir, checkCrc=config.checkCrc).changedFiles
                case _ => config.files map { new LogFile(_) }
            }

            val valueSerializer = config.msgFormat match {
                case `pb` => new ProtoBufSerializer
                case `json` => new JsonSerializer
                case `raw` => new RawSerializer
            }

            val broker = if (config.kafkaUri != null) {
                new LogBroker(config.kafkaUri, valueSerializer)
            } else {
                new LogConsole(valueSerializer)
            }

            run(changedFiles,
                broker,
                zxidRange = config.zxidRange,
                pathPrefix = config.pathPrefix)
        }
    }

    def run(changedFiles: Traversable[LogFile], broker: Broker, zxidRange: Range, pathPrefix: String): Unit = {
        try {
            changedFiles foreach { log =>
                logger.info(s"sync log file ${log.filename} ...")

                log.records filter { r =>
                    (zxidRange contains r.zxid) && r.path.forall(_.startsWith(pathPrefix))
                } foreach { r =>
                    broker.send(r)
                }
            }
        } catch {
            case err: Throwable => {
                logger.error(s"crashed, $err")
                logger.debug({
                    val sw = new StringWriter
                    err.printStackTrace(new PrintWriter(sw))
                    sw.toString
                })
            }
        }
    }
}


