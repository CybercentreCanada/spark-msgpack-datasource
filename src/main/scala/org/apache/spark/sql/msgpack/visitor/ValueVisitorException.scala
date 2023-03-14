package org.apache.spark.sql.msgpack.visitor

import org.apache.spark.sql.msgpack.MessagePackUtil
import org.msgpack.value.Value

class ValueVisitorException(value: Value, context: ValueVisitorContext)
    extends IllegalArgumentException(MessagePackUtil.tracePathError(value, context)) {}
