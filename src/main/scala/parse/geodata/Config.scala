package parse.geodata

import org.apache.spark.{SparkConf, SparkContext}

class Config {

  val sparkConf = new SparkConf()
    .setAppName("example")
    .setMaster("local[*]")
    .set("spark.driver.allowMultipleContext", "true")

  val sc = new SparkContext(sparkConf)

  val path = "/Volumes/DATA/GRID/milano-grid.geojson"

}
