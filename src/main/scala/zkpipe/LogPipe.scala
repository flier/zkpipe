package zkpipe

import java.io.{Closeable, EOFException, File, FileInputStream}
import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.util.Properties
import java.util.zip.Adler32

import com.google.common.io.CountingInputStream
import com.netaporter.uri.Uri
import com.netaporter.uri.dsl._
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.jute.{BinaryInputArchive, Record}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.zookeeper.server.persistence.{FileHeader, FileTxnLog}
import org.apache.zookeeper.server.util.SerializeUtils
import org.apache.zookeeper.txn.TxnHeader
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case class CRCException() extends Exception("CRC doesn't match")

case class EORException() extends Exception("Last transaction was partial")

case class IteratorException() extends Exception("iterator has finished")

class LogRecord(val record: Record, val header: TxnHeader)

class LogFile(val file: File, offset: Long = 0, checkCrc: Boolean = true) extends Closeable {
    require(file.isFile, "Have to be a regular file")
    require(file.canRead, "Have to be readable")

    private val logger = Logger[LogFile]
    private val cis = new CountingInputStream(new FileInputStream(file))
    private val stream = BinaryInputArchive.getArchive(cis)

    lazy val name = file.getName

    val header = new FileHeader()

    header.deserialize(stream, "fileHeader")

    var position = cis.getCount

    if (offset > position) {
        cis.skip(offset-position)

        position = cis.getCount
    }

    def isValid: Boolean = header.getMagic == FileTxnLog.TXNLOG_MAGIC

    val records: Stream[LogRecord] = {
        def next(): Stream[LogRecord] = Try(readRecord()) match {
            case Success(record) => record #:: next()
            case Failure(err) => {
                err match {
                    case _: EOFException => logger.debug("EOF reached")
                    case _: CRCException => logger.warn("CRC doesn't match")
                    case _: EORException => logger.warn("Last transaction was partial.")
                }

                close()

                Stream.empty
            }
        }

        next()
    }

    def readRecord(): LogRecord = {
        val crcValue = stream.readLong("crcValue")
        val buf = stream.readBuffer("txnEntry")

        if (buf.isEmpty) throw new EOFException()

        if (checkCrc) {
            val crc = new Adler32()

            crc.update(buf, 0, buf.length)

            if (crc.getValue != crcValue) throw CRCException()
        }

        val header = new TxnHeader()
        val record = SerializeUtils.deserializeTxn(buf, header)

        if (stream.readByte("EOR") != 'B') throw EORException()

        position = cis.getCount

        new LogRecord(record, header)
    }

    override def close(): Unit = cis.close()
}

class LogBroker(uri: Uri) extends LazyLogging {
    require(uri.scheme.contains("kafka"), "Have to starts with kafka://")

    private lazy val props = {
        val props = new Properties()

        for (
            host <- uri.host;
            port <- uri.port
        ) {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"$host:$port")
        }

        for ( (key, value) <- uri.query.params ) {
            props.put(key, value)
        }

        props
    }

    lazy val topic: String = uri.path

    lazy val consumer = new KafkaConsumer(props)
    lazy val producer = new KafkaProducer(props)
}

class LogWatcher(files: Seq[File]) {
    val logger: Logger = Logger[LogWatcher]

    val watchFiles: mutable.Map[Path, LogFile] = mutable.Map() ++ (files filter { file =>
        file.isFile && file.canRead && new LogFile(file).isValid
    } map { file =>
        (file.toPath, new LogFile(file))
    })

    val watchDirs: Seq[Path] = files map { file =>
        if (file.isDirectory) file else file.getParentFile
    } map { file => file.toPath }

    val watcher: WatchService = FileSystems.getDefault.newWatchService()

    val watchKeys: Map[WatchKey, Path] = watchDirs map { dir =>
        logger.info(s"watching directory `$dir`...")

        (dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY), dir)
    } toMap

    def close(): Unit = {
        for ( (_, logFile) <- watchFiles ) { logFile.close() }
    }

    val changes: Stream[LogFile] = {
        def next(): Stream[LogFile] = {
            val watchKey = watcher.take()

            val watchEvents = watchKey.pollEvents().asScala flatMap { watchEvent =>
                val filename = watchKeys(watchKey).resolve(watchEvent.context.asInstanceOf[Path])

                logger.info(s"file `$filename` {}", watchEvent.kind match {
                    case ENTRY_CREATE => "created"
                    case ENTRY_DELETE => "deleted"
                    case ENTRY_MODIFY => "modified"
                })

                watchEvent.kind match {
                    case ENTRY_CREATE => {
                        Some(watchFiles.getOrElseUpdate(filename, new LogFile(filename.toFile)))
                    }
                    case ENTRY_DELETE => {
                        watchFiles.remove(filename).foreach(_.close())

                        None
                    }
                    case ENTRY_MODIFY => {
                        val logFile = watchFiles.get(filename) match {
                            case Some(logFile) => {
                                logFile.close()

                                new LogFile(logFile.file, offset = logFile.position)
                            }
                            case None => new LogFile(filename.toFile)
                        }

                        watchFiles(filename) = logFile

                        Some(logFile)
                    }
                }
            }

            Stream.concat(watchEvents) #::: next()
        }

        next()
    }
}

case class Config(logFiles: Seq[File] = Seq(),
                  logDir: Option[File] = None,
                  fromZxid: Int = -1,
                  toZxid: Int = Integer.MAX_VALUE,
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

            lazy val watcher = new LogWatcher(config.files)

            run(watcher)
        }
    }

    def run(watcher: LogWatcher) = {
        lazy val t = new Thread(() => {
            logger.info("watcher started")

            while (!Thread.interrupted()) {
                try {
                    for (log <- watcher.changes) {
                        lazy val zxid = if (log.records.isEmpty) -1 else log.records.head.header.getZxid

                        logger.info(s"sync log file `${log.name}` @ zxid=$zxid")
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


