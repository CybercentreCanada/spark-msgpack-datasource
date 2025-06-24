package org.apache.spark.sql.msgpack.suite

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.msgpack.test.data.impl.ComplexData
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.SparkConf

class MessagePackWriterSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.driver.host", "127.0.0.1")


  test("write: ComplexData") {
    val df = spark.read.format("messagepack").load(new ComplexData().write())
    withTempPath(dir => {
      df.write.format("messagepack").save(dir.toString)
      val assertDf = spark.read.format("messagepack").load(dir.toString)
      checkAnswer(assertDf, df)
    })
  }

}
