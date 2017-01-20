package zkpipe

import org.specs2.Specification

import scala.util.Try

class LogRecordSpec extends Specification { val is = s2"""
    LogRecord specification
        when parse emtpy transaction will throw exception       $e1
        when parse invalid transaction will throw exception     $e2
    """

    def e1 = Try(new LogRecord(Array[Byte]())) must beAFailedTry

    def e2 = Try(new LogRecord(Seq.fill(16)(0.toByte).toArray)) must beAFailedTry
}
