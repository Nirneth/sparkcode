package org.norton.SampleAPP
import org.apache.spark.sql.functions._

object isNullEmptyFunction {
  def isNullEmptyFunction(value: String): String = {
    if ( value == null ) {
      return "true"
    }
    return "false"
  }
  val isNullUDF = udf(isNullEmptyFunction _)
}
