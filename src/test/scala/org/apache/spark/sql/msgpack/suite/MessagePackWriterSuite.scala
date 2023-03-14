package org.apache.spark.sql.msgpack.suite

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.msgpack.test.data.impl.ComplexData
import org.apache.spark.sql.test.SharedSparkSession

class MessagePackWriterSuite extends QueryTest with SharedSparkSession {

  test("write: ComplexData") {
    val df = spark.read.format("messagepack").load(new ComplexData().write())
    withTempPath(dir => {
      df.write.format("messagepack").save(dir.toString)
      val assertDf = spark.read.format("messagepack").load(dir.toString)
      checkAnswer(assertDf, df)
    })
  }

}
