package org.apache.spark.sql.msgpack.suite

import org.apache.spark.sql.msgpack.test.data.impl.OneRowData
import org.scalatest.funsuite.AnyFunSuite

class MessagePackDataSuite extends AnyFunSuite {

  test("OneRowData") {
    val oneRow = new OneRowData()
    val filePath = oneRow.write()
    assert(filePath.endsWith("OneRowData.mp"))
    assert(oneRow.onFs === true)
    oneRow.delete()
    assert(oneRow.onFs === false)
  }

  test("OneRowData:compress") {
    val oneRow = new OneRowData()
    val filePath = oneRow.write(compress = true)
    assert(filePath.endsWith("OneRowData.zst"))
    assert(oneRow.onFs === true)
    oneRow.delete()
    assert(oneRow.onFs === false)
  }

}
