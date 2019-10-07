package parse.communication

import java.time.LocalDate

import app.Constants

object CommunicationDataParser {

  var workZonesIndexes = List.empty[Int]
  var relaxZonesIndexes = List.empty[Int]
  var otherZonesIndexes = List.empty[Int]

  def communicationDayFileParse(date: String, month: String): Map[Int, Double] = {

    val config = new Config(date, month)
    val file = config.textFile
    var totalSum: Double = 0.0
    var workSum: Double = 0.0
    var uniqueVal = CommunicationParsableData(file.first())
    var result = Map.empty[Int, Double]

    if (isWorkingDay(uniqueVal.dayOfMonth)) {
      var sum: Double = uniqueVal.callsIn + uniqueVal.callsOut + uniqueVal.smsIn +
        uniqueVal.smsOut + uniqueVal.internet

      file
        .collect()
        .toList
        .foreach(line => {
          val row = CommunicationParsableData(line)
          if (row.sensorID == uniqueVal.sensorID)
            if (row.timeInterval == uniqueVal.timeInterval)
              sum += row.callsIn + row.callsOut + row.smsIn + row.smsOut + row.internet
            else {
              totalSum += sum
              if (isWorkingTime(uniqueVal.timeInterval)) {
                workSum += sum
              }
              uniqueVal = row
              sum = uniqueVal.callsIn + uniqueVal.callsOut + uniqueVal.smsIn +
                uniqueVal.smsOut + uniqueVal.internet
            }
          else {
            result = result + (uniqueVal.sensorID -> workSum / totalSum)
            uniqueVal = row
            sum = uniqueVal.callsIn + uniqueVal.callsOut + uniqueVal.smsIn +
              uniqueVal.smsOut + uniqueVal.internet
            totalSum = 0.0
            workSum = 0.0
          }
        })
    }
    config.sc.stop()
    result
  }

  def isWorkingDay(date: String): Boolean = {
    val celebrations = List("11-10", "12-08", "12-25", "12-26", "12-31")
    val dayOfWeek = LocalDate.of(
      2013,
      (date.charAt(0) - '0') * 10 + (date.charAt(1) - '0'),
      (date.charAt(3) - '0') * 10 + (date.charAt(4) - '0')
    ).getDayOfWeek.getValue

    !(celebrations.contains(date) || (dayOfWeek == 6 || dayOfWeek == 7))
  }

  def isWorkingTime(string: String): Boolean = {
    (string.charAt(0) - '0' == 0 && string.charAt(1) - '0' == 9) ||
      ((string.charAt(0) - '0' == 1 && string.charAt(1) - '0' <= 3) &&
        (string.charAt(0) - '0' == 1 && string.charAt(1) - '0' >= 5) ||
        (string.charAt(0) - '0' == 1 && string.charAt(1) - '0' <= 7))
  }

  def isRestTime(string: String): Boolean = {
    (string.charAt(0) - '0' == 0 && string.charAt(1) - '0' < 9) ||
      (string.charAt(0) - '0' == 1 && string.charAt(1) - '0' > 7) ||
      (string.charAt(0) - '0' == 2 && string.charAt(1) - '0' <= 3)
  }

  def isCarryingTime(string: String): Boolean = {
    string.charAt(0) - '0' == 0 && string.charAt(1) - '0' < 3
  }

  def mapVal(x: Option[(Double, Int)], value: Double): (Double, Int) = x match {
    case Some(pair) => (pair._1 + value, pair._2 + 1)
    case None => (value, 1)
  }

  def setIndexes(): Unit = {

    var res = Map.empty[Int, (Double, Int)]

    (1 to 31).map {
      case e if (e < 10) => {
        communicationDayFileParse(s"11-0$e", "NOVEMBER")
        communicationDayFileParse(s"12-0$e", "DECEMBER")
      }
      case 31 => communicationDayFileParse(s"12-31", "DECEMBER")
      case e => {
        communicationDayFileParse(s"11-$e", "NOVEMBER")
        communicationDayFileParse(s"12-$e", "DECEMBER")
      }
    }.filterNot(_.isEmpty)
      .map(
        mkv => mkv.map(
          kv => {
            res = res.updated(
              kv._1,
              mapVal(res.get(kv._1), kv._2)
            )
          }
        )
      )

    res.foreach(
      m => {
        if (m._2._1 / m._2._2.toDouble > Constants.higher)
          workZonesIndexes = workZonesIndexes :+ m._1
        else if (m._2._1 / m._2._2.toDouble < Constants.lower)
          relaxZonesIndexes = relaxZonesIndexes :+ m._1
        else
          otherZonesIndexes = otherZonesIndexes :+ m._1
      }
    )
  }

}
