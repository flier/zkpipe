package zkpipe

import java.io.{Closeable, PrintWriter, StringWriter}
import java.nio.charset.StandardCharsets.UTF_8

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.Serializer
import org.apache.logging.log4j.core.config.Configurator

import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps, reflectiveCalls}

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
            config.loggingLevel foreach Configurator.setRootLevel

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
                    val kafkaBroker = new KafkaBroker(config.kafkaUri, config.valueSerializer, config.sendQueueSize)

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


