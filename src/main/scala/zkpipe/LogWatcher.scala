package zkpipe

import java.io.{Closeable, File}
import java.nio.file.StandardWatchEventKinds.{ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY}
import java.nio.file.{FileSystems, Path, WatchKey, WatchService}

import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.language.postfixOps

object LogWatcherMetrics {
    val SUBSYSTEM = "watcher"
    val fileChanges: Counter = Counter.build().subsystem(SUBSYSTEM).name("changes").labelNames("dir", "kind").help("watched file changes").register()
}

class LogWatcher(dir: File, checkCrc: Boolean) extends Closeable with LazyLogging{
    import LogWatcherMetrics._

    require(dir.isDirectory, "can only watch directory")

    val watchFiles: mutable.Map[Path, LogFile] = mutable.Map.empty ++
        (dir.listFiles filter { _.isFile } filter { _.canRead } map { file =>
            (dir.toPath.resolve(file.getName), new LogFile(file))
        })

    val watcher: WatchService = FileSystems.getDefault.newWatchService()

    val watchKeys: Map[WatchKey, Path] = Map(dir.toPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY) -> dir.toPath)

    logger.info(s"watching directory ${dir} ...")

    override def close(): Unit = {
        watcher.close()
        watchFiles.values foreach { _.close() }
    }

    val changedFiles: Stream[LogFile] = {
        def next(): Stream[LogFile] = {
            val watchKey = watcher.take()

            val watchEvents = watchKey.pollEvents().asScala flatMap { watchEvent =>
                val dirname = watchKeys(watchKey)
                val filename = dirname.resolve(watchEvent.context.asInstanceOf[Path])

                logger.info(s"file `$filename` {}", watchEvent.kind match {
                    case ENTRY_CREATE => "created"
                    case ENTRY_DELETE => "deleted"
                    case ENTRY_MODIFY => "modified"
                })

                watchEvent.kind match {
                    case ENTRY_CREATE =>
                        fileChanges.labels(dirname.toString, "create").inc()

                        Some(watchFiles.getOrElseUpdate(filename, new LogFile(filename.toFile, checkCrc = checkCrc)))

                    case ENTRY_DELETE =>
                        fileChanges.labels(dirname.toString, "delete").inc()

                        watchFiles.remove(filename).foreach(_.close())

                        None

                    case ENTRY_MODIFY =>
                        fileChanges.labels(dirname.toString, "modified").inc()

                        val logFile = watchFiles.get(filename) match {
                            case Some(log: LogFile) =>
                                log.close()

                                new LogFile(log.file, offset = log.position, checkCrc = checkCrc)

                            case None =>
                                new LogFile(filename.toFile, checkCrc = checkCrc)
                        }

                        watchFiles(filename) = logFile

                        Some(logFile)
                }
            }

            watchKey.reset()

            Stream.concat(watchEvents) #::: next()
        }

        next()
    }
}
