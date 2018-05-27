package com.github.zyz.kafka

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

/**
  * @author zyz
  * @since 2018/5/272
  */
object LogManagerLoadLogs {


  def main(args: Array[String]) {
    val logDirs = Seq[String]("/Users/zyz/Work/data/kafka/0.8.2.2")

    val logs = logDirs.map(new File(_)).toArray

    val pool = Executors.newFixedThreadPool(1)
    for (dir <- logs) {
      val jobs = for {
        dirContent <- Option(dir.listFiles()).toList
        logDir <- dirContent if logDir.isDirectory
      } yield {
        new Runnable {
          override def run(): Unit = {
            println(logDir.getName)
            logDir.listFiles().foreach{ f=>
              println(f.getAbsolutePath)
              println(f.lastModified())
            }
          }
        }
      }
      val ll = jobs.map(pool.submit)
      ll.foreach(_.get())
    }
    pool.shutdown()
    pool.awaitTermination(1, TimeUnit.DAYS)
  }
}