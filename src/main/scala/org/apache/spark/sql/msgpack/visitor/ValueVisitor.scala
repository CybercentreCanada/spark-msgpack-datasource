package org.apache.spark.sql.msgpack.visitor

import org.msgpack.value._

abstract class ValueVisitor {

  def visit(value: Value, context: ValueVisitorContext = new ValueVisitorContext()): Any = {
    (value.getValueType) match {
      case ValueType.MAP =>
        visit(value.asMapValue(), context)
      case ValueType.ARRAY =>
        visit(value.asArrayValue(), context)
      case ValueType.STRING =>
        visit(value.asStringValue(), context)
      case ValueType.INTEGER =>
        visit(value.asIntegerValue(), context)
      case ValueType.FLOAT =>
        visit(value.asFloatValue(), context)
      case ValueType.BOOLEAN =>
        visit(value.asBooleanValue(), context)
      case ValueType.BINARY =>
        visit(value.asBinaryValue(), context)
      case ValueType.NIL =>
        visit(value.asNilValue(), context)
      case ValueType.EXTENSION =>
        visit(value.asExtensionValue(), context)
    }
  }

  protected def visit(value: MapValue, context: ValueVisitorContext): Any;

  protected def visit(value: ArrayValue, context: ValueVisitorContext): Any;

  protected def visit(value: StringValue, context: ValueVisitorContext): Any;

  protected def visit(value: IntegerValue, context: ValueVisitorContext): Any;

  protected def visit(value: FloatValue, context: ValueVisitorContext): Any;

  protected def visit(value: BooleanValue, context: ValueVisitorContext): Any;

  protected def visit(value: BinaryValue, context: ValueVisitorContext): Any;

  protected def visit(value: NilValue, context: ValueVisitorContext): Any;

  protected def visit(value: ExtensionValue, context: ValueVisitorContext): Any;

}
