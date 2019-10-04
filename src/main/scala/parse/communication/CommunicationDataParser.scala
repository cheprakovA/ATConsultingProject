package parse.communication

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CommunicationDataParser {

  val sparkConf = new SparkConf()
    .setAppName("example")
    .setMaster("local[*]")
    .set("spark.driver.allowMultipleContext", "true")

  val sc = new SparkContext(sparkConf)

  val path = "/Volumes/DATA/NOVEMBER/sms-call-internet-mi-2013-11-01.txt"

  val textFile: RDD[String] = sc.textFile(path)

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  def parse(): List[(Int, String, Float)] = {

    var uniqueVal = CommunicationParsableData(textFile.first())
    var result = List.empty[(Int, Long, Float)]

    var sum: Float = uniqueVal.callsIn + uniqueVal.callsOut + uniqueVal.smsIn +
      uniqueVal.smsOut + uniqueVal.internet

    textFile.take(1000).toList.tail
      .foreach(line => {
        val row = CommunicationParsableData(line)
        if (row.sensorID == uniqueVal.sensorID &&
          row.timeInterval == uniqueVal.timeInterval) {
          sum += row.callsIn + row.callsOut + row.smsIn + row.smsOut + row.internet
        } else {
          result = result :+ (
            uniqueVal.sensorID,
            uniqueVal.timeInterval,
            sum
          )
          uniqueVal = row
          sum = uniqueVal.callsIn + uniqueVal.callsOut + uniqueVal.smsIn +
            uniqueVal.smsOut + uniqueVal.internet
        }
      })
      result.map {
        case (i, l, fl) => (
          i,
          new java.text.SimpleDateFormat("HH:mm").
            format(new java.util.Date(l - 3600000)),
          fl
        )
      }
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

  def workingTime(): Unit = {
    parse().filter {
      case (_, str, _) => isWorkingTime(str)
    }.foreach(println)
  }

  def restTime(): Unit = {
    parse().filter {
      case (_, str, _) => isRestTime(str)
    }
  }

  def main(args: Array[String]): Unit = {
    workingTime()
  }

}
