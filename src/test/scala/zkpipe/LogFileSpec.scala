package zkpipe

import java.io.FileOutputStream
import java.nio.file.{Files, Path}

import org.apache.jute.BinaryOutputArchive
import org.apache.zookeeper.server.persistence.{FileHeader, FileTxnLog}
import org.apache.zookeeper.txn.TxnHeader
import org.specs2.Specification
import com.github.nscala_time.time.StaticDateTime.now
import com.github.nscala_time.time.Imports._
import org.apache.zookeeper.ZooDefs.OpCode

import scala.util.Try

class LogFileSpec extends Specification { val is = s2"""
    LogFile specification
        when parse empty file will throw exception      $e1
        when parse log file without records             $e2
        when parse log file with one record             $e3
    """

    def tmpfile: Path = Files.createTempFile("log", ".tmp")

    def e1 = {
        Try(new LogFile(tmpfile.toFile)) must beAFailedTry
    }

    def e2 = {
        val path = tmpfile
        val archive = BinaryOutputArchive.getArchive(new FileOutputStream(path.toFile))

        val header = new FileHeader(FileTxnLog.TXNLOG_MAGIC, 2, 0)

        header.serialize(archive, "fileHeader")

        archive.flush()

        val l = new LogFile(path.toFile)

        l must be_!=(null)
        l.isValid must beTrue
        l.filepath must beEqualTo(path)
        l.closed must beFalse
        l.position() must beEqualTo(16)
        l.firstZxid must beNone
        l.lastZxid must beNone
        l.records.isEmpty must beTrue
    }

    def e3 = {
        val path = tmpfile
        val stream = BinaryOutputArchive.getArchive(new FileOutputStream(path.toFile))

        new FileHeader(FileTxnLog.TXNLOG_MAGIC, 2, 0).serialize(stream, "fileHeader")

        stream.writeLong(0, "crcValue")

        val ts = now()

        new TxnHeader(123, 1, 2, ts.getMillis, OpCode.ping).serialize(stream, "txnHeader")

        stream.writeByte('B'.toByte, "EOR")
        stream.flush()

        val l = new LogFile(path.toFile, checkCrc=false)

        l.records.isEmpty must beFalse
        l.firstZxid must beSome(2)
        l.lastZxid must beNone
    }
}
