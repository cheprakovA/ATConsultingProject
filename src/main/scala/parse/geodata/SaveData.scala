package parse.geodata

import app.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.immutable

object SaveData extends App{

  val tempDataWorking: immutable.Seq[Row] = GEODataParser.createData('w')
  val tempDataRelax: immutable.Seq[Row] = GEODataParser.createData('r')
  val tempDataOther: immutable.Seq[Row] = GEODataParser.createData('o')


  val spark = SparkSession.builder()
    .appName("communication-data-parse")
    .master("local[*]")
    .getOrCreate()

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  fs.delete(new Path(Constants.OUTPUT_PATH +
    Constants.ZONES_COORDS_PATH + Constants.WORKING_ZONES_PATH), true)
  fs.delete(new Path(Constants.OUTPUT_PATH +
    Constants.ZONES_COORDS_PATH + Constants.RELAX_ZONES_PATH), true)
  fs.delete(new Path(Constants.OUTPUT_PATH +
    Constants.ZONES_COORDS_PATH + Constants.OTHER_ZONES_PATH), true)

  var rdd = spark.sparkContext.parallelize(tempDataWorking)
  var df = spark.createDataFrame(rdd, ParsedGeoData.schema)

  df
    .write
    .mode(SaveMode.Append)
    .parquet(Constants.OUTPUT_PATH + Constants.ZONES_COORDS_PATH + Constants.WORKING_ZONES_PATH)


  rdd = spark.sparkContext.parallelize(tempDataRelax)
  df = spark.createDataFrame(rdd, ParsedGeoData.schema)

  df
    .write
    .mode(SaveMode.Append)
    .parquet(Constants.OUTPUT_PATH + Constants.ZONES_COORDS_PATH + Constants.RELAX_ZONES_PATH)

  rdd = spark.sparkContext.parallelize(tempDataOther)
  df = spark.createDataFrame(rdd, ParsedGeoData.schema)

  df
    .write
    .mode(SaveMode.Append)
    .parquet(Constants.OUTPUT_PATH + Constants.ZONES_COORDS_PATH + Constants.OTHER_ZONES_PATH)

}
