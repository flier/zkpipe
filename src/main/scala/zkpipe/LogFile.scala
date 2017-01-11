package zkpipe

import java.io.{Closeable, EOFException, File, FileInputStream}
import java.util.zip.Adler32

import com.google.common.io.CountingInputStream
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter, Gauge, Histogram, Summary}
import org.apache.jute.BinaryInputArchive
import org.apache.zookeeper.server.persistence.{FileHeader, FileTxnLog}
import com.github.nscala_time.time.StaticDateTime.now
import com.github.nscala_time.time.Imports._

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

case class CRCException() extends Exception("CRC doesn't match")

case class EORException() extends Exception("Last transaction was partial")

case class IteratorException() extends Exception("iterator has finished")

object LogFile {
    val SUBSYSTEM: String = "file"
    val opening: Gauge = Gauge.build().subsystem(SUBSYSTEM).name("opening").help("opening files").register()
    val crcErrors: Counter = Counter.build().subsystem(SUBSYSTEM).name("crc_errors").labelNames("filename").help("read record failed, CRC error").register()
    val readBytes: Counter = Counter.build().subsystem(SUBSYSTEM).name("read_bytes").labelNames("filename").help("read bytes").register()
    val readRecords: Counter = Counter.build().subsystem(SUBSYSTEM).name("read_records").labelNames("filename").help("read records").register()
    val size: Summary = Summary.build().subsystem(SUBSYSTEM).name("size").help("record size").register()
    val delay: Histogram = Histogram.build().subsystem(SUBSYSTEM).name("delay").help("sync delay").register()

    val LogFilename: Regex = """log\.(\d+)""".r
}

class LogFile(val file: File, offset: Long = 0, checkCrc: Boolean = true) extends Closeable with LazyLogging {
    import LogFile._

    require(file.isFile, "Have to be a regular file")
    require(file.canRead, "Have to be readable")

    logger.debug(s"opening `$filename` ...")

    private val cis = new CountingInputStream(new FileInputStream(file))
    private val stream = BinaryInputArchive.getArchive(cis)
    private var closed = false

    lazy val filename: String = file.getAbsolutePath

    var firstZxid: Option[Long] = file.getName match {
        case LogFilename(zxid) => Some(zxid.toLong)
        case _ => None
    }

    var lastZxid: Option[Long] = None

    val header = new FileHeader()

    header.deserialize(stream, "fileHeader")

    var position: Long = cis.getCount

    if (offset > position) {
        logger.debug(s"skip to offset $offset")

        cis.skip(offset-position)

        position = cis.getCount
    }

    opening.inc()
    readBytes.labels(filename).inc(position)

    def isValid: Boolean = header.getMagic == FileTxnLog.TXNLOG_MAGIC

    val records: Stream[LogRecord] = {
        def next(): Stream[LogRecord] = Try(readRecord()) match {
            case Success(record) =>
                if (firstZxid.isEmpty) firstZxid = Some(record.zxid)

                lastZxid = Some(record.zxid)

                record #:: next()

            case Failure(err) =>
                err match {
                    case _: EOFException => logger.debug(s"EOF reached @ $position")
                    case e: Exception => logger.warn(s"load `$filename` failed, ${e.getMessage}")
                }

                close()

                Stream.empty
        }

        next()
    }

    def readRecord(): LogRecord = {
        val crcValue = stream.readLong("crcValue")
        val bytes = stream.readBuffer("txnEntry")

        if (bytes.isEmpty) throw new EOFException()

        if (stream.readByte("EOR") != 'B') throw EORException()

        if (checkCrc) {
            val crc = new Adler32()

            crc.update(bytes, 0, bytes.length)

            if (crc.getValue != crcValue) {
                crcErrors.labels(filename).inc()

                throw CRCException()
            }
        }

        val record = new LogRecord(bytes)

        readBytes.labels(filename).inc(cis.getCount - position)
        readRecords.labels(filename).inc()
        size.observe(bytes.length)
        delay.observe((record.time to now).millis)

        position = cis.getCount

        record
    }

    override def close(): Unit = {
        if (!closed) {
            logger.info(s"close `$filename`")

            cis.close()

            opening.dec()

            closed = true
        }
    }
}
