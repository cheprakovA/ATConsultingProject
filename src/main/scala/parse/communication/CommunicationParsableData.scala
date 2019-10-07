package parse.communication

case class CommunicationParsableData(
                                      sensorID: Int,
                                      unixTime: Long,
                                      weekDay: String,
                                      dayOfMonth: String,
                                      timeInterval: String,
                                      countryId: Int,
                                      smsIn: Double,
                                      smsOut: Double,
                                      callsIn: Double,
                                      callsOut: Double,
                                      internet: Double
                                    )

object CommunicationParsableData extends Enumeration {

  val DELIMITER = "\\t"

  var SENSOR_ID, TIME_INTERVAL, COUNTRY_ID, SMS_IN_ACTIVITY,
  SMS_OUT_ACTIVITY, CALL_IN_ACTIVITY, CALL_OUT_ACTIVITY, INTERNET_ACTIVITY = Value

  private def nonEmpty(str: String): Double = str match {
    case "" => 0.0
    case _ => str.toDouble
  }

  def apply(row: String): CommunicationParsableData = {

    val arr = row.split(DELIMITER, -1)

    CommunicationParsableData(
      arr(SENSOR_ID.id).toInt,
      arr(TIME_INTERVAL.id).toLong,
      new java.text.SimpleDateFormat("EEEEE")
        .format(new java.util.Date (arr(TIME_INTERVAL.id).toLong - 3600000)),
      new java.text.SimpleDateFormat("MM-dd")
        .format(new java.util.Date (arr(TIME_INTERVAL.id).toLong - 3600000)),
      new java.text.SimpleDateFormat("HH:mm")
        .format(new java.util.Date (arr(TIME_INTERVAL.id).toLong - 3600000)),
      arr(COUNTRY_ID.id).toInt,
      nonEmpty(arr(SMS_IN_ACTIVITY.id)),
      nonEmpty(arr(SMS_OUT_ACTIVITY.id)),
      nonEmpty(arr(CALL_IN_ACTIVITY.id)),
      nonEmpty(arr(CALL_OUT_ACTIVITY.id)),
      nonEmpty(arr(INTERNET_ACTIVITY.id))
    )

  }
}
