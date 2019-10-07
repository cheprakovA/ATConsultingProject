package parse.geodata

import app.Constants
import org.apache.spark.{SparkConf, SparkContext}

class Config {

  val sparkConf = new SparkConf()
    .setAppName("example")
    .setMaster("local[*]")
    .set("spark.driver.allowMultipleContext", "true")

  val sc = new SparkContext(sparkConf)

  val path = Constants.GRID_PATH

}
