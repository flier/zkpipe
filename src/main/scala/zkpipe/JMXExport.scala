package zkpipe

import java.lang.management.ManagementFactory
import java.util
import javax.management.{MBeanServer, ObjectName}

import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object JMXExport extends LazyLogging {
    logger.info(s"register MBeans to ${ManagementFactory.getRuntimeMXBean.getName}")

    val mBeanServer: MBeanServer = ManagementFactory.getPlatformMBeanServer
}

trait JMXExport {
    import JMXExport._

    implicit def stringToObjectName(name: String): ObjectName = new ObjectName(name)

    def mbean(obj: Object, name: ObjectName = null): Unit = {
        val clazz = obj.getClass

        mBeanServer.registerMBean(obj, if (name != null) name else {
            new ObjectName(clazz.getPackage.getName, "type", clazz.getSimpleName)
        })
    }

    def mbean(obj: Object, props: Map[String, String]): Unit = {
        val clazz = obj.getClass

        mBeanServer.registerMBean(obj,
            new ObjectName(clazz.getPackage.getName,
                new util.Hashtable((Map("type" -> clazz.getSimpleName) ++ props).asJava))
        )
    }
}