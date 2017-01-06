package zkpipe

import org.apache.jute.Record
import org.apache.zookeeper.txn.TxnHeader
import org.joda.time.DateTime

import com.github.nscala_time.time.Imports._

class LogRecord(val record: Record, val header: TxnHeader) {
    lazy val clientId: Long = header.getClientId
    lazy val cxid: Int = header.getCxid
    lazy val zxid: Long = header.getZxid
    lazy val time: DateTime = header.getTime.toDateTime
}
