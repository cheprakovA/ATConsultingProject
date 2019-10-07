package parse.communication

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class Config(date: String, month: String) {

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("example")
    .setMaster("local[*]")
    .set("spark.driver.allowMultipleContext", "true")

  val sc = new SparkContext(sparkConf)

  val textFile: RDD[String] = sc.textFile(
    s"/Volumes/DATA/$month/sms-call-internet-mi-2013-$date.txt"
  )

}
