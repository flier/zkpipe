package zkpipe

import java.io.File

import com.netaporter.uri.Uri
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.Serializer
import scopt.{OptionParser, Read}

import scala.collection.mutable

object MessageFormats extends Enumeration {
    type MessageFormat = Value

    val pb, json, raw = Value
}

import MessageFormats._

case class Config(logFiles: Seq[File] = Seq(),
                  logDir: Option[File] = None,
                  fromZxid: Int = -1,
                  toZxid: Int = Integer.MAX_VALUE,
                  checkCrc: Boolean = true,
                  kafkaUri: Uri = null,
                  msgFormat: MessageFormat = pb) extends LazyLogging
{
    lazy val files: Seq[File] = (logFiles ++ logDir) flatMap { file =>
        if (file.isDirectory)
            file.listFiles
        else if (file.isFile)
            Seq(file)
        else {
            if (!file.exists()) {
                logger.warn(s"skip path `$file` doesn't exists")
            } else {
                logger.warn(s"skip unknown type of file `$file`")
            }

            Seq()
        }
    }
}

object Config {
    def parse(args: Array[String]): Option[Config] = {
        implicit val messageFormatRead: Read[MessageFormat] = Read.reads(MessageFormats withName)
        implicit val uriRead: Read[Uri] = Read.reads(Uri.parse)

        val parser = new OptionParser[Config]("zkpipe") {
            head("zkpipe", "0.1")

            opt[File]('d', "log-dir")
                .valueName("<path>")
                .action((x, c) => c.copy(logDir = Some(x)))
                .validate(c => if (c.isDirectory) success else failure("Option `log-dir` should be a directory"))
                .text("Zookeeper log directory")

            opt[Int]('f', "from")
                .valueName("<zxid>")
                .action((x, c) => c.copy(fromZxid = x))
                .text("sync start from ZooKeeper transaction id")

            opt[Int]('t', "to")
                .valueName("<zxid>")
                .action((x, c) => c.copy(toZxid = x))
                .text("sync end to ZooKeeper transaction id")

            opt[Boolean]("check-crc")
                .action((x, c) => c.copy(checkCrc = x))
                .text("check record CRC correct")

            opt[Uri]('k', "kafka")
                .valueName("<uri>")
                .action((x, c) => c.copy(kafkaUri = x))
                .validate(c => if (c.scheme.contains("kafka")) success else failure("Option `kafka` scheme should be `kafka://`"))
                .text("sync records to Kafka")

            opt[MessageFormat]("msg-format")
                .valueName("<format>")
                .action((x, c) => c.copy(msgFormat = x))
                .text("serialize message in [pb, json, raw] format (default: pb)")

            help("help").abbr("h").text("show usage screen")

            arg[File]("<file>...")
                .unbounded()
                .optional()
                .action( (x, c) => c.copy(logFiles = c.logFiles :+ x) )
                .validate(c => if (c.isFile && c.canRead) success else failure("Option `file` should be a readable file"))
                .text("sync Zookeeper log files")
        }

        parser.parse(args, Config())
    }
}

class LogConsole(serializer: Serializer[LogRecord]) extends Broker with LazyLogging {

}

object LogPipe extends LazyLogging {
    def main(args: Array[String]): Unit = {
        for (config <- Config.parse(args))
        {
            lazy val broker = if (config.kafkaUri != null)
                new LogBroker(config.kafkaUri, valueSerializer = config.msgFormat match {
                    case `pb` => new ProtoBufSerializer
                    case `json` => new JsonSerializer
                    case `raw` => new RawSerializer
                })
            else
                new LogConsole(new JsonSerializer(mutable.Map("pretty" -> true)))

            lazy val watcher = new LogWatcher(config.files, checkCrc=config.checkCrc)

            run(config, broker, watcher)
        }
    }

    def run(config: Config, broker: Broker, watcher: Watcher): Unit = {
        lazy val t = new Thread(() => {
            logger.info("watcher started")

            while (!Thread.interrupted()) {
                try {
                    watcher.changes foreach { log =>
                        lazy val zxid = if (log.records.isEmpty) -1 else log.records.head.header.getZxid

                        logger.info(s"sync log file `${log.name}` @ zxid=$zxid")

                        log.records filter { record =>
                            config.toZxid until config.fromZxid contains record.zxid
                        } foreach { record =>

                        }
                    }
                } catch {
                    case _: InterruptedException =>
                        logger.info("watcher is closing")

                        watcher.close()

                        logger.info("watcher closed")

                        Thread.currentThread.interrupt()

                    case err: Throwable =>
                        logger.error(s"watcher crashed, $err")
                }
            }
        })

        sys.addShutdownHook({
            logger.info("shutdown watcher")

            t.interrupt()
            t.join()
        })

        logger.info("watcher is starting")

        t.start()
    }
}


