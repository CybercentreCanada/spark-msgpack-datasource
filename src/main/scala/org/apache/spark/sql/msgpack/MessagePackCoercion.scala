package org.apache.spark.sql.msgpack

import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructField, StructType}

object MessagePackCoercion {

  def coerce(t1: DataType, t2: DataType): DataType = {
    TypeCoercion
      .findTightestCommonType(t1, t2)
      .orElse {
        (t1, t2) match {
          case (t1: StructType, t2: StructType) =>
            Some(coerceStruct(t1, t2))
          case (t1: ArrayType, t2: ArrayType) =>
            Some(ArrayType(coerce(t1.elementType, t2.elementType)))
          case _ =>
            None
        }
      }
      .getOrElse(
        throw new IllegalArgumentException(s"Unable to coerce ${t1.typeName} and ${t2.typeName}")
      )
  }

  private def coerceStruct(t1: StructType, t2: StructType) = {
    val t1Keys = t1.fieldNames
    val t2Keys = t2.fieldNames
    val fields = (t1Keys ++ t2Keys).distinct.map {
      case k if t1Keys.contains(k) && t2Keys.contains(k) =>
        val field1 = t1.fields(t1.getFieldIndex(k).get)
        val field2 = t2.fields(t2.getFieldIndex(k).get)
        StructField(k, coerce(field1.dataType, field2.dataType), nullable = true)
      case k if t1Keys.contains(k) =>
        t1.fields(t1.getFieldIndex(k).get)
      case k if t2Keys.contains(k) =>
        t2.fields(t2.getFieldIndex(k).get)
    }
    StructType(sort(fields))
  }

  def coerce(types: Array[_ <: DataType]): DataType = {
    var coercedType: DataType = NullType
    val iterator = types.iterator
    while (iterator.hasNext) {
      coercedType = coerce(coercedType, iterator.next)
    }
    coercedType
  }

  def sort(fields: Array[StructField]): Array[StructField] = {
    fields.sortWith((f1, f2) => {
      f1.name.compareTo(f2.name) < 0
    })
  }

}
