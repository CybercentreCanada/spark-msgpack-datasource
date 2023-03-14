package org.apache.spark.sql.msgpack.test.utils

import org.apache.commons.io.FileUtils
import org.apache.spark.util.Utils

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{Date, Timestamp}
import java.util.Calendar

object MessagePackSuiteUtil {

  def timestamp(year: Int, month: Int, day: Int): Timestamp = {
    val cal = Calendar.getInstance
    cal.set(Calendar.YEAR, year)
    cal.set(Calendar.MONTH, month)
    cal.set(Calendar.DAY_OF_MONTH, day)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    new Timestamp(cal.getTimeInMillis);
  }

  def now(nanos: Int = 0): Timestamp = {
    val cal = Calendar.getInstance
    cal.set(Calendar.YEAR, 2019)
    cal.set(Calendar.MONTH, 11)
    cal.set(Calendar.DAY_OF_MONTH, 12)
    cal.set(Calendar.HOUR_OF_DAY, 12)
    cal.set(Calendar.MINUTE, 12)
    cal.set(Calendar.SECOND, 12)
    val ts = new Timestamp(cal.getTimeInMillis);
    ts.setNanos(nanos)
    ts
  }

  def createNonMapRootFile(dir: File): Unit = {
    Utils.tryWithResource {
      FileUtils.forceMkdir(dir)
      val corruptFile = new File(dir, "badpack.mp")
      new BufferedWriter(new FileWriter(corruptFile))
    } { writer =>
      writer.write("corruptors against pheonixes.")
    }
  }

  def trimDays(date: Date): Long = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }

}
