package zkpipe

import java.io.{Closeable, EOFException, File, FileInputStream}
import java.util.zip.Adler32

import com.google.common.io.CountingInputStream
import com.typesafe.scalalogging.Logger
import org.apache.jute.BinaryInputArchive
import org.apache.zookeeper.server.persistence.{FileHeader, FileTxnLog}
import org.apache.zookeeper.server.util.SerializeUtils
import org.apache.zookeeper.txn.TxnHeader

import scala.util.{Failure, Success, Try}

case class CRCException() extends Exception("CRC doesn't match")

case class EORException() extends Exception("Last transaction was partial")

case class IteratorException() extends Exception("iterator has finished")

class LogFile(val file: File, offset: Long = 0, checkCrc: Boolean = true) extends Closeable {
    require(file.isFile, "Have to be a regular file")
    require(file.canRead, "Have to be readable")

    private val logger = Logger[LogFile]
    private val cis = new CountingInputStream(new FileInputStream(file))
    private val stream = BinaryInputArchive.getArchive(cis)

    lazy val name: String = file.getName

    val header = new FileHeader()

    header.deserialize(stream, "fileHeader")

    var position: Long = cis.getCount

    if (offset > position) {
        cis.skip(offset-position)

        position = cis.getCount
    }

    def isValid: Boolean = header.getMagic == FileTxnLog.TXNLOG_MAGIC

    val records: Stream[LogRecord] = {
        def next(): Stream[LogRecord] = Try(readRecord()) match {
            case Success(record) => record #:: next()
            case Failure(err) =>
                err match {
                    case _: EOFException => logger.debug("EOF reached")
                    case e: Exception => logger.warn(e.getMessage)
                }

                close()

                Stream.empty
        }

        next()
    }

    def readRecord(): LogRecord = {
        val crcValue = stream.readLong("crcValue")
        val buf = stream.readBuffer("txnEntry")

        if (buf.isEmpty) throw new EOFException()

        if (stream.readByte("EOR") != 'B') throw EORException()

        if (checkCrc) {
            val crc = new Adler32()

            crc.update(buf, 0, buf.length)

            if (crc.getValue != crcValue) throw CRCException()
        }

        val header = new TxnHeader()
        val record = SerializeUtils.deserializeTxn(buf, header)

        position = cis.getCount

        new LogRecord(header, record)
    }

    override def close(): Unit = cis.close()
}
