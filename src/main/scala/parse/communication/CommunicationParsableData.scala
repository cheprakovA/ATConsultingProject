package parse.communication

import org.apache.spark.sql.types.{StructField, _}

object CommunicationParsableData extends Enumeration {

  val DELIMITER = "\\t"

  var SENSOR_ID, TIME_INTERVAL, COUNTRY_ID, SMS_IN_ACTIVITY,
  SMS_OUT_ACTIVITY, CALL_IN_ACTIVITY, CALL_OUT_ACTIVITY, INTERNET_ACTIVITY = Value

  val structType = StructType(
    Seq(
      StructField(SENSOR_ID.toString, IntegerType),
      StructField(TIME_INTERVAL.toString, LongType),
      StructField(COUNTRY_ID.toString, IntegerType),
      StructField(SMS_IN_ACTIVITY.toString, FloatType),
      StructField(SMS_OUT_ACTIVITY.toString, FloatType),
      StructField(CALL_IN_ACTIVITY.toString, FloatType),
      StructField(CALL_OUT_ACTIVITY.toString, FloatType),
      StructField(INTERNET_ACTIVITY.toString, FloatType)
    )
  )

  private def nonEmpty(str: String): Float = str match {
    case "" => 0
    case _ => str.toFloat
  }

  def apply(row: String): CommunicationParsableData = {

    val arr = row.split(DELIMITER, -1)

    CommunicationParsableData(
      arr(SENSOR_ID.id).toInt,
      arr(TIME_INTERVAL.id).toLong,
      arr(COUNTRY_ID.id).toInt,
      nonEmpty(arr(SMS_IN_ACTIVITY.id)),
      nonEmpty(arr(SMS_OUT_ACTIVITY.id)),
      nonEmpty(arr(CALL_IN_ACTIVITY.id)),
      nonEmpty(arr(CALL_OUT_ACTIVITY.id)),
      nonEmpty(arr(INTERNET_ACTIVITY.id))
    )

  }
}

case class CommunicationParsableData(
                                      sensorID: Int,
                                      timeInterval: Long,
                                      countryId: Int,
                                      smsIn: Float,
                                      smsOut: Float,
                                      callsIn: Float,
                                      callsOut: Float,
                                      internet: Float
                                    )
