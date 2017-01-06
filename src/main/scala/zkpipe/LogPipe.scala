package zkpipe

import java.io.File

import com.netaporter.uri.dsl._
import com.typesafe.scalalogging.LazyLogging
import scopt.OptionParser

import scala.language.postfixOps

case class Config(logFiles: Seq[File] = Seq(),
                  logDir: Option[File] = None,
                  fromZxid: Int = -1,
                  toZxid: Int = Integer.MAX_VALUE,
                  checkCrc: Boolean = true,
                  kafkaUri: String = null) extends LazyLogging
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
        val parser = new OptionParser[Config]("zkpipe") {
            head("zkpipe", "0.1")

            opt[File]('d', "log-dir")
                .valueName("<path>")
                .action( (x, c) => c.copy(logDir = Some(x)) )
                .text("Zookeeper log directory")

            opt[Int]('f', "from")
                .valueName("<zxid>")
                .action( (x, c) => c.copy(fromZxid = x) )
                .text("sync start from ZooKeeper transaction id")

            opt[Int]('t', "to")
                .valueName("<zxid>")
                .action( (x, c) => c.copy(toZxid = x) )
                .text("sync end to ZooKeeper transaction id")

            opt[Boolean]("check-crc")
                .action( (x, c) => c.copy(checkCrc = x) )
                .text("check record CRC correct")

            opt[String]('k', "kafka")
                .valueName("<uri>")
                .action( (x, c) => c.copy(kafkaUri = x))
                .text("sync records to Kafka")

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

object LogPipe extends LazyLogging {
    def main(args: Array[String]): Unit = {
        for (config <- Config.parse(args))
        {
            lazy val broker = new LogBroker(config.kafkaUri)

            lazy val watcher = new LogWatcher(config.files, checkCrc=config.checkCrc)

            run(config, broker, watcher)
        }
    }

    def run(config: Config, broker: LogBroker, watcher: LogWatcher): Unit = {
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


