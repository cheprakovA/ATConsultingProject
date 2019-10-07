package parse.geodata

import app.Parameters
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.immutable

object SaveData extends App {

  val tempDataWorking: immutable.Seq[Row] = GEODataParser.createData('w')
  val tempDataRelax: immutable.Seq[Row] = GEODataParser.createData('r')
  val tempDataOther: immutable.Seq[Row] = GEODataParser.createData('o')

  val spark = SparkSession.builder()
    .appName("communication-data-parse")
    .master("local[*]")
    .getOrCreate()

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  fs.delete(new Path(Parameters.OUTPUT_PATH + Parameters.ZONES_COORDS_PATH + Parameters.WORKING_ZONES_PATH), true)
  fs.delete(new Path(Parameters.OUTPUT_PATH + Parameters.ZONES_COORDS_PATH + Parameters.RELAX_ZONES_PATH), true)
  fs.delete(new Path(Parameters.OUTPUT_PATH + Parameters.ZONES_COORDS_PATH + Parameters.OTHER_ZONES_PATH), true)

  var rdd = spark.sparkContext.parallelize(tempDataWorking)
  var df = spark.createDataFrame(rdd, ParsedGeoData.schema)

  df
    .write
    .mode(SaveMode.Append)
    .parquet(Parameters.OUTPUT_PATH + Parameters.ZONES_COORDS_PATH + Parameters.WORKING_ZONES_PATH)


  rdd = spark.sparkContext.parallelize(tempDataRelax)
  df = spark.createDataFrame(rdd, ParsedGeoData.schema)

  df
    .write
    .mode(SaveMode.Append)
    .parquet(Parameters.OUTPUT_PATH + Parameters.ZONES_COORDS_PATH + Parameters.RELAX_ZONES_PATH)

  rdd = spark.sparkContext.parallelize(tempDataOther)
  df = spark.createDataFrame(rdd, ParsedGeoData.schema)

  df
    .write
    .mode(SaveMode.Append)
    .parquet(Parameters.OUTPUT_PATH + Parameters.ZONES_COORDS_PATH + Parameters.OTHER_ZONES_PATH)

//  val dff = spark
//    .read
//    .option("mergeSchema", "true")
//    .parquet(Parameters.OUTPUT_PATH + Parameters.ZONES_COORDS_PATH + Parameters.OTHER_ZONES_PATH)
//
//
//  dff.show(20)
}
