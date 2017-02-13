package zkpipe

import java.io.{Closeable, EOFException, File, FileInputStream}
import java.nio.file.Path
import java.util.zip.Adler32

import com.google.common.io.CountingInputStream
import com.typesafe.scalalogging.LazyLogging
import org.apache.jute.BinaryInputArchive
import org.apache.zookeeper.server.persistence.{FileHeader, FileSnap, FileTxnLog}
import com.github.nscala_time.time.StaticDateTime.now
import com.github.nscala_time.time.Imports._
import nl.grons.metrics.scala.{Counter, DefaultInstrumented, Histogram, Meter}
import org.apache.zookeeper.data.StatPersisted
import org.apache.zookeeper.server.ReferenceCountedACLCache

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

case class CRCException() extends Exception("CRC doesn't match")

case class EORException() extends Exception("Last transaction was partial")

case class IteratorException() extends Exception("iterator has finished")

object LogFile extends DefaultInstrumented {
    val SUBSYSTEM: String = "file"

    val crcErrors: Counter = metrics.counter("crc-errors", SUBSYSTEM)
    val readBytes: Meter = metrics.meter("read-bytes", SUBSYSTEM)
    val readRecords: Meter = metrics.meter("read-records", SUBSYSTEM)
    val recordSize: Histogram = metrics.histogram("record-size", SUBSYSTEM)
    val recordLatency: Histogram = metrics.histogram("read-latency", SUBSYSTEM)

    val LogFilename: Regex = """log\.(\d+)""".r
}

class LogFile(val file: File,
              @BeanProperty
              val offset: Long = 0,
              @BooleanBeanProperty
              val checkCrc: Boolean = true)
    extends JMXExport with LogFileMBean with Closeable with LazyLogging
{
    import LogFile._

    require(file.isFile, "Have to be a regular file")
    require(file.canRead, "Have to be readable")

    @BeanProperty
    val filename: String = file.getAbsolutePath

    lazy val filepath: Path = file.getAbsoluteFile.toPath

    logger.debug(s"opening `$filename` ...")

    private val cis: CountingInputStream = new CountingInputStream(new FileInputStream(file))
    private val stream: BinaryInputArchive = BinaryInputArchive.getArchive(cis)

    @BooleanBeanProperty
    var closed: Boolean = false

    val header = new FileHeader()

    header.deserialize(stream, "fileHeader")

    @BeanProperty
    var position: Long = cis.getCount

    readBytes.mark(position)

    @BooleanBeanProperty
    def isLog: Boolean = header.getMagic == FileTxnLog.TXNLOG_MAGIC

    @BooleanBeanProperty
    def isSnapshot: Boolean = header.getMagic == FileSnap.SNAP_MAGIC

    var aclCache: ReferenceCountedACLCache = null

    if (offset > position) {
        logger.debug(s"skip to offset $offset")

        cis.skip(offset-position)

        position = cis.getCount
    } else if (isSnapshot) {
        for (_ <- 0 until stream.readInt("count")) {
            val id = stream.readLong("id")
            val timeout = stream.readInt("timeout")

            logger.trace(s"session `$id` with timeout $timeout")
        }

        aclCache = new ReferenceCountedACLCache()
        aclCache.deserialize(stream)

        position = cis.getCount
    }

    def skipToEnd: LogRecord = {
        val last = records.last

        logger.info(s"`$filename` skip to end @ ${last.zxid}")

        last
    }

    val records: Stream[LogRecord] = {
        def next(): Stream[LogRecord] = Try(if (isLog) readRecord() else readDataNode()) match {
            case Success(txn) =>
                lastZxid = Some(txn.zxid)

                txn #:: next()

            case Failure(err) =>
                err match {
                    case _: EOFException => logger.debug(s"EOF reached @ $position, zxid=$lastZxid")
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
                crcErrors += 1

                throw CRCException()
            }
        }

        val record: TransactionLog = new TransactionLog(bytes)

        readBytes.mark(cis.getCount - position)
        readRecords.mark()
        recordSize += bytes.length
        recordLatency += (record.time to now).millis

        position = cis.getCount

        record
    }

    def readDataNode(): LogRecord = {
        val path = stream.readString("path")

        stream.startRecord("node")
        val data = stream.readBuffer("data")
        val acl = stream.readLong("acl")
        val stat = new StatPersisted
        stat.deserialize(stream, "statpersisted")
        stream.endRecord("node")

        position = cis.getCount

        aclCache.addUsage(acl)

        if (path == "/") {
            aclCache.purgeUnused()

            cis.close()
        }

        new DataNode(path, data, aclCache.convertLong(acl), stat)
    }

    var firstZxid: Option[Long] = Try(records.head.zxid).toOption

    var lastZxid: Option[Long] = None

    val mBean: JMXBean = registerMBean(this, Map("name" -> file.getName))

    override def close(): Unit = {
        if (!closed) {
            logger.info(s"close `$filename`")

            cis.close()

            if (mBean != null) mBean.unregister()

            closed = true
        }
    }

    override def getFirstZxid: Long = firstZxid.getOrElse(-1)

    override def getLastZxid: Long = lastZxid.getOrElse(-1)
}
