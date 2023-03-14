package org.apache.spark.sql.msgpack.test.data.impl

import org.apache.spark.sql.msgpack.test.data.MessagePackData
import org.apache.spark.sql.types._

import java.time.{LocalDateTime, ZoneOffset}

class ComplexData extends MessagePackData {

  override def schema(): StructType =
    StructType(
      Seq(
        StructField("k1", StringType, nullable = true),
        StructField("k2", ArrayType(DoubleType), nullable = true),
        StructField(
          "k3",
          StructType(
            Seq(
              StructField("k3.1", StringType, nullable = true),
              StructField(
                "k3.2",
                ArrayType(
                  StructType(
                    Seq(
                      StructField("k3.2[item]", BinaryType, nullable = true)
                    )
                  )
                ),
                nullable = true
              ),
              StructField("k3.3", LongType, nullable = true)
            )
          ),
          nullable = true
        ),
        StructField("k4", TimestampType, nullable = true),
        StructField("k5", BinaryType, nullable = true)
      )
    )

  override def pack(): Unit = {
    packer
      .packMapHeader(5)
      .packString("k1")
      .packString("v1")
      .packString("k2")
      .packArrayHeader(3)
      .packFloat(1f)
      .packFloat(2f)
      .packFloat(3f)
      .packString("k3")
      .packMapHeader(3)
      .packString("k3.1")
      .packString("v31")
      .packString("k3.2")
      .packArrayHeader(2)
      .packMapHeader(1)
      .packString("k3.2[item]")
      .packBinaryHeader("k3.2[0].value".getBytes.length)
      .writePayload("k3.2[0].value".getBytes())
      .packMapHeader(1)
      .packString("k3.2[item]")
      .packBinaryHeader("k3.2[1].value".getBytes.length)
      .writePayload("k3.2[1].value".getBytes())
      .packString("k3.3")
      .packInt(33)
      .packString("k4")
      .packTimestamp(LocalDateTime.of(2023, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC))
      .packString("k5")
      .packExtensionTypeHeader(1, "extension_value".getBytes.length)
      .writePayload("extension_value".getBytes)
  }
}
