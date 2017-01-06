package zkpipe

import org.apache.jute.Record
import org.apache.zookeeper.txn.TxnHeader
import org.apache.zookeeper.ZooDefs.OpCode
import com.github.nscala_time.time.Imports._

class LogRecord(val header: TxnHeader, val record: Record) {
    object TxnType extends Enumeration {
        type Type = Value

        val notification = Value(OpCode.notification)
        val create = Value(OpCode.create)
        val delete = Value(OpCode.delete)
        val exists = Value(OpCode.exists)
        val getData = Value(OpCode.getData)
        val setData = Value(OpCode.setData)
        val getACL = Value(OpCode.getACL)
        val setACL = Value(OpCode.setACL)
        val getChildren = Value(OpCode.getChildren)
        val sync = Value(OpCode.sync)
        val ping = Value(OpCode.ping)
        val getChildren2 = Value(OpCode.getChildren2)
        val check = Value(OpCode.check)
        val multi = Value(OpCode.multi)
        val create2 = Value(OpCode.create2)
        val reconfig = Value(OpCode.reconfig)
        val checkWatches = Value(OpCode.checkWatches)
        val removeWatches = Value(OpCode.removeWatches)
        val createContainer = Value(OpCode.createContainer)
        val deleteContainer = Value(OpCode.deleteContainer)
        val auth = Value(OpCode.auth)
        val setWatches = Value(OpCode.setWatches)
        val sasl = Value(OpCode.sasl)
        val createSession = Value(OpCode.createSession)
        val closeSession = Value(OpCode.closeSession)
        val error = Value(OpCode.error)
    }

    import TxnType._

    lazy val session: Long = header.getClientId
    lazy val cxid: Int = header.getCxid
    lazy val zxid: Long = header.getZxid
    lazy val time: DateTime = header.getTime.toDateTime
    lazy val opcode: Type = apply(header.getType)
}
