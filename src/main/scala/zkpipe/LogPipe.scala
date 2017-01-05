package zkpipe

import java.io.{Closeable, EOFException, File, FileInputStream}
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.{FileSystems, Path, WatchKey, WatchService}
import java.util.zip.Adler32

import com.google.common.io.CountingInputStream
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.jute.{BinaryInputArchive, Record}
import org.apache.zookeeper.server.persistence.{FileHeader, FileTxnLog}
import org.apache.zookeeper.server.util.SerializeUtils
import org.apache.zookeeper.txn.TxnHeader
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.Try

case class CRCException() extends Exception("CRC doesn't match")

case class EORException() extends Exception("Last transaction was partial")

case class IteratorException() extends Exception("iterator has finished")

class LogRecord(val record: Record, val header: TxnHeader)

class LogFile(val file: File, offset: Int = 0, checkCrc: Boolean = true) extends Iterable[LogRecord] with Closeable {
    require(file.isFile, "Have to be a regular file")
    require(isValid(), "Have to be a Zookeeper log file")
    require(file.canRead, "Have to be readable")

    val logger = Logger[LogFile]
    val cis = new CountingInputStream(new FileInputStream(file))
    val stream = BinaryInputArchive.getArchive(cis)

    val header = new FileHeader()

    header.deserialize(stream, "fileHeader")

    val pos = cis.getCount

    if (offset > pos) cis.skip(offset-pos)

    def isValid() = header.getMagic == FileTxnLog.TXNLOG_MAGIC

    def readRecord(): LogRecord = {
        val crcValue = stream.readLong("crcValue")
        val buf = stream.readBuffer("txnEntry")

        if (buf.isEmpty) throw new EOFException()

        if (checkCrc) {
            val crc = new Adler32()

            crc.update(buf, 0, buf.length)

            if (crc.getValue != crcValue) throw new CRCException()
        }

        val header = new TxnHeader()
        val record = SerializeUtils.deserializeTxn(buf, header)

        if (stream.readByte("EOR") != 'B') throw new EORException()

        new LogRecord(record, header)
    }

    class RecordIterator extends Iterator[LogRecord] {
        var record: Option[LogRecord] = tryRead()

        def tryRead(): Option[LogRecord] = Try(readRecord()).toOption

        def hasNext: Boolean = record.isDefined

        def next(): LogRecord = {
            val cur = record.get

            record = tryRead()

            cur
        }
    }

    lazy val iter = new RecordIterator()

    def iterator(): Iterator[LogRecord] = iter

    def close(): Unit = cis.close()
}

case class Config(logFiles: Seq[File] = Seq(),
                  logDir: Option[File] = None,
                  fromZxid: Int = -1,
                  toZxid: Int = Integer.MAX_VALUE)

object LogPipe extends LazyLogging {
    def main(args: Array[String]): Unit = {
        for (config <- parseCmdLine(args))
        {
            val files = (config.logFiles ++ config.logDir) flatMap { file =>
                if (file.isDirectory)
                    file.listFiles
                else if (file.isFile)
                    Seq(file)
                else {
                    if (!file.exists()) {
                        logger.warn("skip path `{}` doesn't exists", file)
                    } else {
                        logger.warn("skip unknown type of file `{}`", file)
                    }

                    Seq()
                }
            } toSet

            val watchFiles = mutable.Map() ++ (files filter { file =>
                file.isFile && file.canRead && new LogFile(file).isValid
            } map { file =>
                (file.toPath, new LogFile(file))
            })

            val watchDirs = files map { file =>
                if (file.isDirectory) file else file.getParentFile
            } map { file => file.toPath }

            val (watcher, watchKeys) = watchLogDirs(watchDirs)

            while (watchKeys.nonEmpty) {
                val watchKey = watcher.take()

                watchKey.pollEvents().asScala foreach { watchEvent =>
                    val filename = watchKeys(watchKey).resolve(watchEvent.context.asInstanceOf[Path])

                    logger.info("file `{}` {}", filename, watchEvent.kind match {
                        case ENTRY_CREATE => "created"
                        case ENTRY_DELETE => "deleted"
                        case ENTRY_MODIFY => "modified"
                    })

                    watchEvent.kind match {
                        case ENTRY_CREATE => watchFiles.getOrElseUpdate(filename, new LogFile(filename.toFile))
                        case ENTRY_DELETE => watchFiles.remove(filename).map(f => f.close())
                        case ENTRY_MODIFY => watchFiles(filename)
                    }
                }

                watchKey.reset()
            }
        }
    }

    def parseCmdLine(args: Array[String]): Option[Config] = {
        val parser = new OptionParser[Config]("zkpipe") {
            head("zkpipe", "0.1")

            opt[File]('d', "log-dir").valueName("<path>").
                action( (x, c) => c.copy(logDir = Some(x)) ).
                text("Zookeeper log directory")
            opt[Int]('f', "from").valueName("<zxid>").action( (x, c) =>
                c.copy(fromZxid = x) ).text("sync from ZooKeeper transaction id")
            opt[Int]('t', "to").valueName("<zxid>").action( (x, c) =>
                c.copy(toZxid = x) ).text("sync to ZooKeeper transaction id")

            help("help").abbr("h").text("show usage screen")

            arg[File]("<file>...").unbounded().optional().action( (x, c) =>
                c.copy(logFiles = c.logFiles :+ x) ).text("sync Zookeeper log files")
        }

        parser.parse(args, Config())
    }

    def watchLogDirs(dirs: Set[Path]): (WatchService, Map[WatchKey, Path]) = {
        val watcher = FileSystems.getDefault.newWatchService()

        (watcher, dirs map { dir =>
            logger.info("watching directory `{}`...", dir)

            (dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY), dir)
        } toMap)
    }
}


