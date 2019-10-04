package parse.geodata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

object GEODataParser {

  val sparkConf = new SparkConf()
    .setAppName("example")
    .setMaster("local[*]")
    .set("spark.driver.allowMultipleContext", "true")

  val sc = new SparkContext(sparkConf)

  val path = "/Volumes/DATA/GRID/milano-grid.geojson"

  val textFile: RDD[String] = sc.textFile(path)
  val jsonStr = textFile.collect().mkString("")

  def parse() = {
    for {
      Some(M(map)) <- List(JSON.parseFull(jsonStr))
      M(crs) = map("crs")
      S(crs_type) = crs("type")
      M(properties) = crs("properties")
      S(name) = properties("name")
      S(_type) = map("type")
      L(features) = map("features")
      M(feature) <- features
      M(geometry) = feature("geometry")
      S(geometry_type) = geometry("type")
      LL(coordinates) = geometry("coordinates")
      LD(coordinate) <- coordinates
      S(feature_type) = feature("type")
      D(feature_id) = feature("id")
      M(feature_properties) = feature("properties")
      D(cell_id) = feature_properties("cellId")
    } yield
      cell_id
  }


  def main(args: Array[String]): Unit = {
    println("==========================")
    parse()
      .foreach(println)
  }

}
